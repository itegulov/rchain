package coop.rchain.blockstorage
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import cats.Monad
import cats.implicits._
import cats.effect.concurrent.MVar
import cats.effect.{Concurrent, ExitCase, Resource, Sync}
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore.BlockHash
import coop.rchain.casper.protocol.BlockMessage
import org.lmdbjava.Txn.NotReadyException
import org.lmdbjava._

final class LMDBIndexBlockStore[F[_]] private (
    lock: MVar[F, Unit],
    indexEnv: Env[ByteBuffer],
    indexBlockOffset: Dbi[ByteBuffer],
    blockBodiesFileResource: Resource[F, RandomAccessFile]
)(implicit syncF: Sync[F])
    extends BlockStore[F] {

  private[this] def withTxn[R](txnThunk: => Txn[ByteBuffer])(f: Txn[ByteBuffer] => R): F[R] =
    syncF.bracketCase(syncF.delay(txnThunk)) { txn =>
      syncF.delay {
        val r = f(txn)
        txn.commit()
        r
      }
    } {
      case (txn, ExitCase.Completed) => syncF.delay(txn.close())
      case (txn, _) =>
        syncF.delay {
          try {
            txn.abort()
          } catch {
            case ex: NotReadyException =>
              ex.printStackTrace()
              TxnOps.manuallyAbortTxn(txn)
            // vide: rchain/rspace/src/main/scala/coop/rchain/rspace/LMDBOps.scala
          }
          txn.close()
        }
    }

  private[this] def withWriteTxn(f: Txn[ByteBuffer] => Unit): F[Unit] =
    withTxn(indexEnv.txnWrite())(f)

  private[this] def withReadTxn[R](f: Txn[ByteBuffer] => R): F[R] =
    withTxn(indexEnv.txnRead())(f)

  implicit class RichBlockHash(byteVector: BlockHash) {
    def toDirectByteBuffer: ByteBuffer = {
      val buffer: ByteBuffer = ByteBuffer.allocateDirect(byteVector.size)
      byteVector.copyTo(buffer)
      // TODO: get rid of this:
      buffer.flip()
      buffer
    }
  }

  def get(blockHash: BlockHash): F[Option[BlockMessage]] =
    for {
      _ <- lock.take
      result <- blockBodiesFileResource.use { f =>
                 withReadTxn { txn =>
                   Option(indexBlockOffset.get(txn, blockHash.toDirectByteBuffer))
                     .map(_.getLong)
                     .map { offset =>
                       f.seek(offset)
                       val size  = f.readInt()
                       val bytes = Array.fill[Byte](size)(0)
                       f.readFully(bytes)
                       f.write(bytes)
                       BlockMessage.parseFrom(ByteString.copyFrom(bytes).newCodedInput())
                     }
                 }
               }
      _ <- lock.put(())
    } yield result
  def find(p: BlockHash => Boolean): F[Seq[(BlockHash, BlockMessage)]] =
    ???
  def put(f: => (BlockHash, BlockMessage)): F[Unit] =
    ???
  def asMap(): F[Map[BlockHash, BlockMessage]] =
    ???
  def clear(): F[Unit] =
    ???
  def close(): F[Unit] =
    ???
}

object LMDBIndexBlockStore {
  final case class Config(
    indexPath: Path,
    indexMapSize: Long,
    blockBodiesPath: Path,
    indexMaxDbs: Int = 1,
    indexMaxReaders: Int = 126,
    indexNoTls: Boolean = true
  )

  def apply[F[_]: Sync: Concurrent: Monad](config: Config): F[LMDBIndexBlockStore[F]] = {
    if (Files.notExists(config.indexPath)) Files.createDirectories(config.indexPath)

    val flags = if (config.indexNoTls) List(EnvFlags.MDB_NOTLS) else List.empty
    val env = Env
      .create()
      .setMapSize(config.indexMapSize)
      .setMaxDbs(config.indexMaxDbs)
      .setMaxReaders(config.indexMaxReaders)
      .open(config.indexPath.toFile, flags: _*) //TODO this is a bracket

    val blocks: Dbi[ByteBuffer] = env.openDbi(s"blocks", DbiFlags.MDB_CREATE) //TODO this is a bracket
    for {
      lock <- MVar.of[F, Unit](())
      blockBodiesFileResource = Resource.fromAutoCloseable(
        new RandomAccessFile(config.blockBodiesPath.toFile, "rw").pure[F])
    } yield new LMDBIndexBlockStore(lock, env, blocks, blockBodiesFileResource)
  }
}
