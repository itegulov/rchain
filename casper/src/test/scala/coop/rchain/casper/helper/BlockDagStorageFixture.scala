package coop.rchain.casper.helper

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.util.zip.CRC32

import cats.effect.{Concurrent, Resource, Sync}
import cats.syntax.functor._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockDagRepresentation.Validator
import coop.rchain.blockstorage._
import coop.rchain.casper.protocol.BlockMessage

import coop.rchain.catscontrib.TaskContrib.TaskOps
import coop.rchain.metrics.Metrics
import coop.rchain.metrics.Metrics.MetricsNOP
import coop.rchain.rspace.Context
import coop.rchain.shared.Log
import org.scalatest.{BeforeAndAfter, Suite}
import coop.rchain.shared.PathOps.RichPath
import monix.eval.Task
import monix.execution.Scheduler
import org.lmdbjava.{Env, EnvFlags}

trait BlockDagStorageFixture extends BeforeAndAfter { self: Suite =>
  val scheduler = Scheduler.fixedPool("block-dag-storage-fixture-scheduler", 4)

  def withStorage[R](f: BlockStore[Task] => IndexedBlockDagStorage[Task] => Task[R]): R = {
    val testProgram = BlockDagStorageTestFixture.createDirectories[Task].use {
      case (blockStorageDir, blockDagStorageDir) =>
        implicit val metrics = new MetricsNOP[Task]()
        implicit val log     = new Log.NOPLog[Task]()
        BlockDagStorageTestFixture.createBlockStorage[Task](blockStorageDir).use { blockStore =>
          for {
            blockDagStorage        <- BlockDagStorageTestFixture.createBlockDagStorage(blockDagStorageDir)
            indexedBlockDagStorage <- IndexedBlockDagStorage.create(blockDagStorage)
            result                 <- f(blockStore)(indexedBlockDagStorage)
          } yield result
        }
    }
    testProgram.unsafeRunSync(scheduler)
  }
}

object BlockDagStorageTestFixture {
  def blockDagStorageDir: Path = Files.createTempDirectory("casper-block-dag-storage-test-")
  def blockStorageDir: Path    = Files.createTempDirectory("casper-block-storage-test-")

  def writeInitialLatestMessages(
      latestMessagesData: Path,
      latestMessagesCrc: Path,
      latestMessages: Map[Validator, BlockMessage]
  ): Unit = {
    val data = latestMessages
      .foldLeft(ByteString.EMPTY) {
        case (byteString, (validator, block)) =>
          byteString.concat(validator).concat(block.blockHash)
      }
      .toByteArray
    val crc = new CRC32()
    latestMessages.foreach {
      case (validator, block) =>
        crc.update(validator.concat(block.blockHash).toByteArray)
    }
    val crcByteBuffer = ByteBuffer.allocate(8)
    crcByteBuffer.putLong(crc.getValue)
    Files.write(latestMessagesData, data)
    Files.write(latestMessagesCrc, crcByteBuffer.array())
  }

  def env(
      path: Path,
      mapSize: Long,
      flags: List[EnvFlags] = List(EnvFlags.MDB_NOTLS)
  ): Env[ByteBuffer] =
    Env
      .create()
      .setMapSize(mapSize)
      .setMaxDbs(8)
      .setMaxReaders(126)
      .open(path.toFile, flags: _*)

  val mapSize: Long = 1024L * 1024L * 100L

  def createBlockStorage[F[_]: Concurrent: Metrics: Sync: Log](
      blockStorageDir: Path
  ): Resource[F, BlockStore[F]] = {
    val env = Context.env(blockStorageDir, mapSize)
    FileLMDBIndexBlockStore.createUnsafe[F](env, blockStorageDir)
  }

  def createBlockDagStorage(blockDagStorageDir: Path)(
      implicit log: Log[Task]
  ): Task[BlockDagStorage[Task]] =
    BlockDagFileStorage.create[Task](
      BlockDagFileStorage.Config(
        blockDagStorageDir.resolve("latest-messages-data"),
        blockDagStorageDir.resolve("latest-messages-crc"),
        blockDagStorageDir.resolve("block-metadata-data"),
        blockDagStorageDir.resolve("block-metadata-crc"),
        blockDagStorageDir.resolve("equivocations-tracker-data"),
        blockDagStorageDir.resolve("equivocations-tracker-crc"),
        blockDagStorageDir.resolve("invalid-blocks-data"),
        blockDagStorageDir.resolve("invalid-blocks-crc"),
        blockDagStorageDir.resolve("checkpoints"),
        blockDagStorageDir.resolve("block-number-index"),
        mapSize
      )
    )

  def createDirectories[F[_]: Concurrent]: Resource[F, (Path, Path)] =
    Resource.make[F, (Path, Path)] {
      Sync[F].delay {
        (
          Files.createTempDirectory("casper-block-storage-test-"),
          Files.createTempDirectory("casper-block-dag-storage-test-")
        )
      }
    } {
      case (blockStoreDir, blockDagDir) =>
        Sync[F].delay {
          blockStoreDir.recursivelyDelete()
          blockDagDir.recursivelyDelete()
        }
    }
}
