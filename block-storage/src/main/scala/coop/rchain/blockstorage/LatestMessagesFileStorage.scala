package coop.rchain.blockstorage
import java.io._
import java.nio.file.Path

import cats.Monad
import cats.effect.concurrent.{MVar, Ref}
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.LatestMessagesStorage.{BlockHash, Validator}
import coop.rchain.shared.Log

import scala.collection.mutable.ArrayBuffer

final class LatestMessagesFileStorage[F[_]: Monad: Concurrent: Sync: Log] private (
    lock: MVar[F, Unit],
    latestMessagesRef: Ref[F, Map[Validator, BlockHash]],
    indexRef: Ref[F, Map[Validator, Long]],
    dataFileResource: Resource[F, RandomAccessFile]
) extends LatestMessagesStorage[F] {
  def access[A](fa: F[Map[Validator, BlockHash] => A]): F[A] =
    for {
      _              <- lock.take
      latestMessages <- latestMessagesRef.get
      result         <- fa.map(_(latestMessages))
      _              <- lock.put(())
    } yield result

  def insert(validator: Validator, blockHash: BlockHash): F[Unit] =
    for {
      _      <- lock.take
      _      <- updateFile(validator, blockHash)
      result <- latestMessagesRef.update(_ + (validator -> blockHash))
      _      <- lock.put(())
    } yield result

  private def updateFile(validator: Validator, blockHash: BlockHash): F[Unit] =
    for {
      index <- indexRef.get
      result <- index.get(validator) match {
                 case None =>
                   dataFileResource.use { dataFile =>
                     dataFile.seek(dataFile.length)
                     val position = dataFile.getFilePointer
                     dataFile.write(validator.concat(blockHash).toByteArray)
                     indexRef.update(_ + (validator -> position))
                   }
                 case Some(position) =>
                   dataFileResource.use { dataFile =>
                     dataFile.seek(position)
                     dataFile.write(blockHash.toByteArray)
                     ().pure[F]
                   }
               }
    } yield result
}

object LatestMessagesFileStorage {
  final case class Config(
      dataPath: Path,
      checksumPath: Path
  )

  private def readLatestMessagesList[F[_]: Monad: Concurrent](
      dataFileResource: Resource[F, RandomAccessFile]
  ): F[List[(Long, Validator, BlockHash)]] =
    dataFileResource.use { dataFile =>
      val buffer = ArrayBuffer.empty[(Long, Validator, BlockHash)]
      dataFile.seek(0)
      while (true) {
        val validatorPk = Array.fill[Byte](32)(0)
        val blockHash   = Array.fill[Byte](32)(0)
        val position    = dataFile.getFilePointer
        dataFile.readFully(validatorPk)
        dataFile.readFully(blockHash)
        buffer.append((position, ByteString.copyFrom(validatorPk), ByteString.copyFrom(blockHash)))
      }
      buffer.toList.pure[F]
    }

  def apply[F[_]: Monad: Concurrent: Sync: Log](config: Config): F[LatestMessagesFileStorage[F]] =
    for {
      lock <- MVar.of[F, Unit](())
      dataFileResource = Resource.fromAutoCloseable(
        new RandomAccessFile(config.path.toFile, "rw").pure[F])
      latestMessagesList <- readLatestMessagesList(dataFileResource)
      latestMessagesMap  = latestMessagesList.map { case (_, v, b) => v -> b }.toMap
      indexMap           = latestMessagesList.map { case (i, v, _) => v -> i }.toMap
      latestMessagesRef  <- Ref.of[F, Map[Validator, BlockHash]](latestMessagesMap)
      indexRef           <- Ref.of[F, Map[Validator, Long]](indexMap)
    } yield new LatestMessagesFileStorage[F](lock, latestMessagesRef, indexRef, dataFileResource)
}
