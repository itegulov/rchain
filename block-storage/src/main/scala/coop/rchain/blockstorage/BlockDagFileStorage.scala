package coop.rchain.blockstorage

import java.io._
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.stream.Collectors

import cats.Monad
import cats.implicits._
import cats.effect.{Concurrent, Resource, Sync}
import cats.effect.concurrent.Semaphore
import cats.mtl.MonadState
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockDagFileStorage.{Checkpoint, CheckpointedDagInfo}
import coop.rchain.blockstorage.BlockDagRepresentation.Validator
import coop.rchain.blockstorage.BlockStore.BlockHash
import coop.rchain.blockstorage.util.BlockMessageUtil.{blockNumber, bonds, parentHashes}
import coop.rchain.blockstorage.util.{BlockMessageUtil, Crc32, TopologicalSortUtil}
import coop.rchain.blockstorage.util.byteOps._
import coop.rchain.casper.protocol.BlockMessage
import coop.rchain.catscontrib.Capture
import coop.rchain.crypto.codec.Base16
import coop.rchain.models.BlockMetadata
import coop.rchain.shared.{AtomicMonadState, Log, LogSource}
import monix.execution.atomic.AtomicAny

import scala.ref.WeakReference
import scala.util.matching.Regex
import collection.JavaConverters._

private final case class BlockDagFileStorageState[F[_]: Sync](
    latestMessages: Map[Validator, BlockHash],
    childMap: Map[BlockHash, Set[BlockHash]],
    dataLookup: Map[BlockHash, BlockMetadata],
    topoSort: Vector[Vector[BlockHash]],
    sortOffset: Long,
    checkpoints: List[Checkpoint],
    latestMessagesLogOutputStream: OutputStream,
    latestMessagesLogSize: Int,
    latestMessagesCrc: Crc32[F],
    blockMetadataLogOutputStream: OutputStream,
    blockMetadataCrc: Crc32[F]
)

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // TODO remove!!
final class BlockDagFileStorage[F[_]: Concurrent: Sync: Log: BlockStore] private (
    lock: Semaphore[F],
    latestMessagesDataFilePath: Path,
    latestMessagesCrcFilePath: Path,
    latestMessagesLogMaxSizeFactor: Int,
    blockMetadataLogPath: Path,
    blockMetadataCrcPath: Path,
    state: MonadState[F, BlockDagFileStorageState[F]]
) extends BlockDagStorage[F] {
  private implicit val logSource = LogSource(BlockDagFileStorage.getClass)

  private[this] def getLatestMessages: F[Map[Validator, BlockHash]] =
    state.get.map(_.latestMessages)
  private[this] def getChildMap: F[Map[BlockHash, Set[BlockHash]]] =
    state.get.map(_.childMap)
  private[this] def getDataLookup: F[Map[BlockHash, BlockMetadata]] =
    state.get.map(_.dataLookup)
  private[this] def getTopoSort: F[Vector[Vector[BlockHash]]] =
    state.get.map(_.topoSort)
  private[this] def getSortOffset: F[Long] =
    state.get.map(_.sortOffset)
  private[this] def getCheckpoints: F[List[Checkpoint]] =
    state.get.map(_.checkpoints)
  private[this] def getLatestMessagesLogOutputStream: F[OutputStream] =
    state.get.map(_.latestMessagesLogOutputStream)
  private[this] def getLatestMessagesLogSize: F[Int] =
    state.get.map(_.latestMessagesLogSize)
  private[this] def getLatestMessagesCrc: F[Crc32[F]] =
    state.get.map(_.latestMessagesCrc)
  private[this] def getBlockMetadataLogOutputStream: F[OutputStream] =
    state.get.map(_.blockMetadataLogOutputStream)
  private[this] def getBlockMetadataCrc: F[Crc32[F]] =
    state.get.map(_.blockMetadataCrc)

  private[this] def setLatestMessages(v: Map[Validator, BlockHash]): F[Unit] =
    state.modify(s => s.copy(latestMessages = v))
  private[this] def setChildMap(v: Map[BlockHash, Set[BlockHash]]): F[Unit] =
    state.modify(s => s.copy(childMap = v))
  private[this] def setDataLookup(v: Map[BlockHash, BlockMetadata]): F[Unit] =
    state.modify(s => s.copy(dataLookup = v))
  private[this] def setTopoSort(v: Vector[Vector[BlockHash]]): F[Unit] =
    state.modify(s => s.copy(topoSort = v))
  private[this] def setSortOffset(v: Long): F[Unit] =
    state.modify(s => s.copy(sortOffset = v))
  private[this] def setCheckpoints(v: List[Checkpoint]): F[Unit] =
    state.modify(s => s.copy(checkpoints = v))
  private[this] def setLatestMessagesLogOutputStream(v: OutputStream): F[Unit] =
    state.modify(s => s.copy(latestMessagesLogOutputStream = v))
  private[this] def setLatestMessagesLogSize(v: Int): F[Unit] =
    state.modify(s => s.copy(latestMessagesLogSize = v))
  private[this] def setLatestMessagesCrc(v: Crc32[F]): F[Unit] =
    state.modify(s => s.copy(latestMessagesCrc = v))
  private[this] def setBlockMetadataLogOutputStream(v: OutputStream): F[Unit] =
    state.modify(s => s.copy(blockMetadataLogOutputStream = v))
  private[this] def setBlockMetadataCrc(v: Crc32[F]): F[Unit] =
    state.modify(s => s.copy(blockMetadataCrc = v))

  private[this] def modifyLatestMessages(
      f: Map[Validator, BlockHash] => Map[Validator, BlockHash]
  ): F[Unit] =
    state.modify(s => s.copy(latestMessages = f(s.latestMessages)))
  private[this] def modifyChildMap(
      f: Map[BlockHash, Set[BlockHash]] => Map[BlockHash, Set[BlockHash]]
  ): F[Unit] =
    state.modify(s => s.copy(childMap = f(s.childMap)))
  private[this] def modifyDataLookup(
      f: Map[BlockHash, BlockMetadata] => Map[BlockHash, BlockMetadata]
  ): F[Unit] =
    state.modify(s => s.copy(dataLookup = f(s.dataLookup)))
  private[this] def modifyTopoSort(
      f: Vector[Vector[BlockHash]] => Vector[Vector[BlockHash]]
  ): F[Unit] =
    state.modify(s => s.copy(topoSort = f(s.topoSort)))
  private[this] def modifySortOffset(f: Long => Long): F[Unit] =
    state.modify(s => s.copy(sortOffset = f(s.sortOffset)))
  private[this] def modifyCheckpoints(f: List[Checkpoint] => List[Checkpoint]): F[Unit] =
    state.modify(s => s.copy(checkpoints = f(s.checkpoints)))
  private[this] def modifyLatestMessagesLogOutputStream(f: OutputStream => OutputStream): F[Unit] =
    state.modify(s => s.copy(latestMessagesLogOutputStream = f(s.latestMessagesLogOutputStream)))
  private[this] def modifyLatestMessagesLogSize(f: Int => Int): F[Unit] =
    state.modify(s => s.copy(latestMessagesLogSize = f(s.latestMessagesLogSize)))
  private[this] def modifyLatestMessagesCrc(f: Crc32[F] => Crc32[F]): F[Unit] =
    state.modify(s => s.copy(latestMessagesCrc = f(s.latestMessagesCrc)))
  private[this] def modifyBlockMetadataLogOutputStream(f: OutputStream => OutputStream): F[Unit] =
    state.modify(s => s.copy(blockMetadataLogOutputStream = f(s.blockMetadataLogOutputStream)))
  private[this] def modifyBlockMetadataCrc(f: Crc32[F] => Crc32[F]): F[Unit] =
    state.modify(s => s.copy(blockMetadataCrc = f(s.blockMetadataCrc)))

  private case class FileDagRepresentation(
      latestMessagesMap: Map[Validator, BlockHash],
      childMap: Map[BlockHash, Set[BlockHash]],
      dataLookup: Map[BlockHash, BlockMetadata],
      topoSortVector: Vector[Vector[BlockHash]],
      sortOffset: Long
  ) extends BlockDagRepresentation[F] {
    def children(blockHash: BlockHash): F[Option[Set[BlockHash]]] =
      for {
        result <- childMap.get(blockHash) match {
                   case Some(children) =>
                     Option(children).pure[F]
                   case None =>
                     for {
                       blockOpt <- BlockStore[F].get(blockHash)
                       result <- blockOpt match {
                                  case Some(block) =>
                                    val number = blockNumber(block)
                                    if (number >= sortOffset) {
                                      none[Set[BlockHash]].pure[F]
                                    } else {
                                      lock.withPermit(
                                        for {
                                          oldDagInfo <- loadCheckpoint(number)
                                        } yield oldDagInfo.flatMap(_.childMap.get(blockHash))
                                      )
                                    }
                                  case None => none[Set[BlockHash]].pure[F]
                                }
                     } yield result
                 }
      } yield result
    def lookup(blockHash: BlockHash): F[Option[BlockMetadata]] =
      dataLookup
        .get(blockHash)
        .fold(
          BlockStore[F].get(blockHash).map(_.map(BlockMetadata.fromBlock))
        )(blockMetadata => Option(blockMetadata).pure[F])
    def contains(blockHash: BlockHash): F[Boolean] =
      dataLookup.get(blockHash).fold(BlockStore[F].contains(blockHash))(_ => true.pure[F])
    def topoSort(startBlockNumber: Long): F[Vector[Vector[BlockHash]]] =
      if (startBlockNumber >= sortOffset) {
        val offset = startBlockNumber - sortOffset
        assert(offset.isValidInt)
        topoSortVector.drop(offset.toInt).pure[F]
      } else if (sortOffset - startBlockNumber + topoSortVector.length < Int.MaxValue) { // Max Vector length
        lock.withPermit(
          for {
            checkpoints          <- getCheckpoints
            checkpointsWithIndex = checkpoints.zipWithIndex
            checkpointsToLoad    = checkpointsWithIndex.filter(startBlockNumber < _._1.end)
            checkpointsDagInfos <- checkpointsToLoad.traverse {
                                    case (startingCheckpoint, index) =>
                                      loadCheckpointDagInfo(startingCheckpoint, index)
                                  }
            topoSortPrefix = checkpointsDagInfos.toVector.flatMap { checkpointsDagInfo =>
              val offset = startBlockNumber - checkpointsDagInfo.sortOffset
              // offset is always a valid Int since the method result's length was validated before
              checkpointsDagInfo.topoSort.drop(offset.toInt) // negative drops are ignored
            }
            result = topoSortPrefix ++ topoSortVector
          } yield result
        )
      } else {
        Sync[F].raiseError(
          TopoSortLengthIsTooBig(sortOffset - startBlockNumber + topoSortVector.length)
        )
      }
    def topoSortTail(tailLength: Int): F[Vector[Vector[BlockHash]]] = {
      val startBlockNumber = Math.max(0L, sortOffset - (tailLength - topoSortVector.length))
      topoSort(startBlockNumber)
    }
    def deriveOrdering(startBlockNumber: Long): F[Ordering[BlockMetadata]] =
      topoSort(startBlockNumber).map { topologicalSorting =>
        val order = topologicalSorting.flatten.zipWithIndex.toMap
        Ordering.by(b => order(b.blockHash))
      }
    def latestMessageHash(validator: Validator): F[Option[BlockHash]] =
      latestMessagesMap.get(validator).pure[F]
    def latestMessage(validator: Validator): F[Option[BlockMetadata]] =
      latestMessagesMap.get(validator).flatTraverse(lookup)
    def latestMessageHashes: F[Map[Validator, BlockHash]] =
      latestMessagesMap.pure[F]
    def latestMessages: F[Map[Validator, BlockMetadata]] =
      latestMessagesMap.toList
        .traverse {
          case (validator, hash) => lookup(hash).map(validator -> _.get)
        }
        .map(_.toMap)
  }

  private def loadDagInfo(checkpoint: Checkpoint): F[CheckpointedDagInfo] = {
    val checkpointDataInputResource = Resource.fromAutoCloseable(
      Sync[F].delay { new RandomAccessFile(checkpoint.path.toFile, "r") }
    )
    for {
      blockMetadataList <- checkpointDataInputResource.use { checkpointDataInput =>
                            BlockDagFileStorage.readDataLookupData(checkpointDataInput)
                          }
      dataLookup = blockMetadataList.toMap
      childMap   = BlockDagFileStorage.extractChildMap(blockMetadataList)
      topoSort   = BlockDagFileStorage.extractTopoSort(blockMetadataList)
    } yield CheckpointedDagInfo(childMap, dataLookup, topoSort, checkpoint.start)
  }

  private def loadCheckpointDagInfo(checkpoint: Checkpoint, index: Int): F[CheckpointedDagInfo] =
    checkpoint.dagInfo.flatMap(_.get) match {
      case Some(dagInfo) =>
        dagInfo.pure[F]
      case None =>
        for {
          loadedDagInfo <- loadDagInfo(checkpoint)
          newCheckpoint = checkpoint.copy(dagInfo = Some(WeakReference(loadedDagInfo)))
          _             <- modifyCheckpoints(_.patch(index, List(newCheckpoint), 1))
        } yield loadedDagInfo
    }

  private def loadCheckpoint(offset: Long): F[Option[CheckpointedDagInfo]] =
    for {
      checkpoints <- getCheckpoints
      neededCheckpoint = checkpoints.zipWithIndex.find {
        case (c, _) => c.start <= offset && offset < c.end
      }
      result <- neededCheckpoint match {
                 case None =>
                   Log[F].warn(
                     s"Requested a block with block number $offset, but there is no checkpoint for it"
                   ) *> None.pure[F]
                 case Some((checkpoint, i)) =>
                   loadCheckpointDagInfo(checkpoint, i).map(Option(_))
               }
    } yield result

  private def updateLatestMessagesFile(validators: List[Validator], blockHash: BlockHash): F[Unit] =
    for {
      latestMessagesCrc <- getLatestMessagesCrc
      _ <- validators.traverse_ { validator =>
            val toAppend = validator.concat(blockHash).toByteArray
            for {
              latestMessagesLogOutputStream <- getLatestMessagesLogOutputStream
              _                             <- Sync[F].delay { latestMessagesLogOutputStream.write(toAppend) }
              _                             <- Sync[F].delay { latestMessagesLogOutputStream.flush() }
              _ <- latestMessagesCrc.update(toAppend).flatMap { _ =>
                    updateLatestMessagesCrcFile(latestMessagesCrc)
                  }
            } yield ()
          }
      _ <- modifyLatestMessagesLogSize(_ + 1)
    } yield ()

  private def updateLatestMessagesCrcFile(newCrc: Crc32[F]): F[Unit] =
    for {
      newCrcBytes <- newCrc.bytes
      _ <- Sync[F].delay {
            val tmpCrc =
              Files.createTempFile("rchain-block-dag-file-storage-latest-messages-", "-crc")
            Files.write(tmpCrc, newCrcBytes)
            Files.move(tmpCrc, latestMessagesCrcFilePath, StandardCopyOption.REPLACE_EXISTING)
          }
    } yield ()

  private def replaceFile(newFile: Path, oldFile: Path): F[Unit] =
    Sync[F].delay {
      Files.move(
        newFile,
        oldFile,
        StandardCopyOption.REPLACE_EXISTING
      )
    }

  private def createTmpFile(prefix: String, suffix: String): F[Path] =
    Sync[F].delay {
      Files.createTempFile(
        prefix,
        suffix
      )
    }

  private def squashLatestMessagesDataFile(): F[Unit] =
    for {
      latestMessages                <- getLatestMessages
      latestMessagesLogOutputStream <- getLatestMessagesLogOutputStream
      _                             = latestMessagesLogOutputStream.close()
      tmpSquashedData               <- createTmpFile("rchain-block-dag-store-latest-messages-", "-squashed-data")
      tmpSquashedCrc                <- createTmpFile("rchain-block-dag-store-latest-messages-", "-squashed-crc")
      dataByteBuffer                = ByteBuffer.allocate(64 * latestMessages.size)
      _ <- latestMessages.toList.traverse_ {
            case (validator, blockHash) =>
              Sync[F].delay {
                dataByteBuffer.put(validator.toByteArray)
                dataByteBuffer.put(blockHash.toByteArray)
              }
          }
      _                <- Sync[F].delay { Files.write(tmpSquashedData, dataByteBuffer.array()) }
      squashedCrc      = Crc32.empty[F]()
      _                <- squashedCrc.update(dataByteBuffer.array())
      squashedCrcBytes <- squashedCrc.bytes
      _                <- Sync[F].delay { Files.write(tmpSquashedCrc, squashedCrcBytes) }
      _                <- replaceFile(tmpSquashedData, latestMessagesDataFilePath)
      _                <- replaceFile(tmpSquashedCrc, latestMessagesCrcFilePath)
      _ <- setLatestMessagesLogOutputStream(
            new FileOutputStream(latestMessagesDataFilePath.toFile, true)
          )
      _ <- setLatestMessagesCrc(squashedCrc)
      _ <- setLatestMessagesLogSize(0)
    } yield ()

  private def squashLatestMessagesDataFileIfNeeded(): F[Unit] =
    for {
      latestMessages        <- getLatestMessages
      latestMessagesLogSize <- getLatestMessagesLogSize
      result <- if (latestMessagesLogSize > latestMessages.size * latestMessagesLogMaxSizeFactor) {
                 squashLatestMessagesDataFile()
               } else {
                 ().pure[F]
               }
    } yield result

  private def updateDataLookupFile(blockMetadata: BlockMetadata): F[Unit] =
    for {
      dataLookupCrc          <- getBlockMetadataCrc
      blockBytes             = blockMetadata.toByteString
      toAppend               = blockBytes.size.toByteString.concat(blockBytes).toByteArray
      dataLookupOutputStream <- getBlockMetadataLogOutputStream
      _                      <- Sync[F].delay { dataLookupOutputStream.write(toAppend) }
      _                      <- Sync[F].delay { dataLookupOutputStream.flush() }
      _                      <- dataLookupCrc.update(toAppend)
      _                      <- updateDataLookupCrcFile(dataLookupCrc)
    } yield ()

  private def updateDataLookupCrcFile(newCrc: Crc32[F]): F[Unit] =
    for {
      newCrcBytes <- newCrc.bytes
      _ <- Sync[F].delay {
            val tmpCrc = Files.createTempFile("rchain-block-dag-file-storage-data-lookup-", "-crc")
            Files.write(tmpCrc, newCrcBytes)
            Files.move(tmpCrc, blockMetadataCrcPath, StandardCopyOption.REPLACE_EXISTING)
          }
    } yield ()

  private def representation: F[BlockDagRepresentation[F]] =
    for {
      latestMessages <- getLatestMessages
      childMap       <- getChildMap
      dataLookup     <- getDataLookup
      topoSort       <- getTopoSort
      sortOffset     <- getSortOffset
    } yield FileDagRepresentation(latestMessages, childMap, dataLookup, topoSort, sortOffset)

  def getRepresentation: F[BlockDagRepresentation[F]] =
    lock.withPermit(representation)

  def insert(block: BlockMessage): F[BlockDagRepresentation[F]] =
    lock.withPermit(
      for {
        alreadyStored <- getDataLookup.map(_.contains(block.blockHash))
        _ <- if (alreadyStored) {
              Log[F].warn(s"Block ${Base16.encode(block.blockHash.toByteArray)} is already stored")
            } else {
              for {
                _             <- squashLatestMessagesDataFileIfNeeded()
                blockMetadata = BlockMetadata.fromBlock(block)
                _             = assert(block.blockHash.size == 32)
                _             <- modifyDataLookup(_.updated(block.blockHash, blockMetadata))
                _ <- modifyChildMap(
                      childMap =>
                        parentHashes(block)
                          .foldLeft(childMap) {
                            case (acc, p) =>
                              val currChildren = acc.getOrElse(p, Set.empty[BlockHash])
                              acc.updated(p, currChildren + block.blockHash)
                          }
                          .updated(block.blockHash, Set.empty[BlockHash])
                    )
                _ <- modifyTopoSort(topoSort => TopologicalSortUtil.update(topoSort, 0L, block))
                //Block which contains newly bonded validators will not
                //have those validators in its justification
                newValidators = bonds(block)
                  .map(_.validator)
                  .toSet
                  .diff(block.justifications.map(_.validator).toSet)
                newValidatorsWithSender <- if (block.sender.isEmpty) {
                                            // Ignore empty sender for special cases such as genesis block
                                            Log[F].warn(
                                              s"Block ${Base16.encode(block.blockHash.toByteArray)} sender is empty"
                                            ) *> newValidators.pure[F]
                                          } else if (block.sender.size() == 32) {
                                            (newValidators + block.sender).pure[F]
                                          } else {
                                            Sync[F].raiseError[Set[ByteString]](
                                              BlockSenderIsMalformed(block)
                                            )
                                          }
                _ <- modifyLatestMessages { latestMessages =>
                      newValidatorsWithSender.foldLeft(latestMessages) {
                        //Update new validators with block in which
                        //they were bonded (i.e. this block)
                        case (acc, v) => acc.updated(v, block.blockHash)
                      }
                    }
                _ <- updateLatestMessagesFile(
                      (newValidators + block.sender).toList,
                      block.blockHash
                    )
                _ <- updateDataLookupFile(blockMetadata)
              } yield ()
            }
        dag <- representation
      } yield dag
    )

  def checkpoint(): F[Unit] =
    ().pure[F]

  def clear(): F[Unit] =
    lock.withPermit(
      for {
        latestMessagesLogOutputStream <- getLatestMessagesLogOutputStream
        _                             <- Sync[F].delay { latestMessagesLogOutputStream.close() }
        blockMetadataLogOutputStream  <- getBlockMetadataLogOutputStream
        _                             <- Sync[F].delay { blockMetadataLogOutputStream.close() }
        _ <- Sync[F].delay {
              Files.write(latestMessagesDataFilePath, Array.emptyByteArray)
            }
        _ <- Sync[F].delay {
              Files.write(blockMetadataLogPath, Array.emptyByteArray)
            }
        newLatestMessagesCrc      = Crc32.empty[F]()
        newLatestMessagesCrcBytes <- newLatestMessagesCrc.bytes
        _ <- Sync[F].delay {
              Files.write(latestMessagesCrcFilePath, newLatestMessagesCrcBytes)
            }
        newBlockMetadataCrc      = Crc32.empty[F]()
        newBlockMetadataCrcBytes <- newBlockMetadataCrc.bytes
        _ <- Sync[F].delay {
              Files.write(blockMetadataCrcPath, newBlockMetadataCrcBytes)
            }
        _ <- setDataLookup(Map.empty)
        _ <- setChildMap(Map.empty)
        _ <- setTopoSort(Vector.empty)
        _ <- setLatestMessages(Map.empty)
        _ <- setLatestMessagesLogOutputStream(
              new FileOutputStream(latestMessagesDataFilePath.toFile)
            )
        _ <- setBlockMetadataLogOutputStream(
              new FileOutputStream(blockMetadataLogPath.toFile)
            )
        _ <- setLatestMessagesLogSize(0)
        _ <- setLatestMessagesCrc(newLatestMessagesCrc)
        _ <- setBlockMetadataCrc(newBlockMetadataCrc)
      } yield ()
    )

  def close(): F[Unit] =
    lock.withPermit(
      for {
        latestMessagesLogOutputStream <- getLatestMessagesLogOutputStream
        _                             <- Sync[F].delay { latestMessagesLogOutputStream.close() }
        blockMetadataLogOutputStream  <- getBlockMetadataLogOutputStream
        _                             <- Sync[F].delay { blockMetadataLogOutputStream.close() }
      } yield ()
    )
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // TODO remove
object BlockDagFileStorage {
  private implicit val logSource       = LogSource(BlockDagFileStorage.getClass)
  private val checkpointPattern: Regex = "([0-9]+)-([0-9]+)".r

  final case class Config(
      latestMessagesLogPath: Path,
      latestMessagesCrcPath: Path,
      blockMetadataLogPath: Path,
      blockMetadataCrcPath: Path,
      checkpointsDirPath: Path,
      latestMessagesLogMaxSizeFactor: Int = 10
  )

  private[blockstorage] final case class CheckpointedDagInfo(
      childMap: Map[BlockHash, Set[BlockHash]],
      dataLookup: Map[BlockHash, BlockMetadata],
      topoSort: Vector[Vector[BlockHash]],
      sortOffset: Long
  )

  private[blockstorage] final case class Checkpoint(
      start: Long,
      end: Long,
      path: Path,
      dagInfo: Option[WeakReference[CheckpointedDagInfo]]
  )

  private def readCrc[F[_]: Sync: Log](crcPath: Path): F[Long] =
    for {
      bytes      <- Sync[F].delay { crcPath.toFile.createNewFile(); Files.readAllBytes(crcPath) }
      byteBuffer = ByteBuffer.wrap(bytes)
      result <- Sync[F].delay { byteBuffer.getLong() }.handleErrorWith {
                 case _: BufferUnderflowException =>
                   for {
                     _ <- Log[F].warn(s"CRC file $crcPath did not contain a valid CRC value")
                   } yield 0
                 case exception =>
                   Sync[F].raiseError(exception)
               }
    } yield result

  private def calculateLatestMessagesCrc[F[_]: Monad](
      latestMessagesList: List[(Validator, BlockHash)]
  ): Crc32[F] =
    Crc32[F](
      latestMessagesList
        .foldLeft(ByteString.EMPTY) {
          case (byteString, (validator, blockHash)) =>
            byteString.concat(validator.concat(blockHash))
        }
        .toByteArray
    )

  private def readLatestMessagesData[F[_]: Sync: Log](
      latestMessagesDataDi: DataInput
  ): F[(List[(Validator, BlockHash)], Int)] = {
    def readRec(
        result: List[(Validator, BlockHash)],
        logSize: Int
    ): F[(List[(Validator, BlockHash)], Int)] = {
      val validatorPk = Array.fill[Byte](32)(0)
      val blockHash   = Array.fill[Byte](32)(0)
      for {
        validatorPkRead <- Sync[F].delay { latestMessagesDataDi.readFully(validatorPk) }.attempt
        blockHashRead   <- Sync[F].delay { latestMessagesDataDi.readFully(blockHash) }.attempt
        result <- (validatorPkRead, blockHashRead) match {
                   case (Right(_), Right(_)) =>
                     val pair = (ByteString.copyFrom(validatorPk), ByteString.copyFrom(blockHash))
                     readRec(
                       pair :: result,
                       logSize + 1
                     )
                   case (Left(_: EOFException), Left(_: EOFException)) =>
                     (result.reverse, logSize).pure[F]
                   case (Right(_), Left(e: EOFException)) =>
                     for {
                       _      <- Log[F].error("Latest messages log is malformed")
                       result <- Sync[F].raiseError[(List[(Validator, BlockHash)], Int)](e)
                     } yield result
                   case (Right(_), Left(e)) =>
                     Sync[F].raiseError(e)
                   case (Left(e), _) =>
                     Sync[F].raiseError(e)
                 }
      } yield result
    }
    readRec(List.empty, 0)
  }

  private def validateLatestMessagesData[F[_]: Monad](
      latestMessagesRaf: RandomAccessFile,
      readLatestMessagesCrc: Long,
      latestMessagesCrcPath: Path,
      latestMessagesList: List[(Validator, BlockHash)]
  ): F[(Map[Validator, BlockHash], Crc32[F])] = {
    val fullCalculatedCrc = calculateLatestMessagesCrc[F](latestMessagesList)
    fullCalculatedCrc.value.flatMap { fullCalculatedCrcValue =>
      if (fullCalculatedCrcValue == readLatestMessagesCrc) {
        (latestMessagesList.toMap, fullCalculatedCrc).pure[F]
      } else {
        val withoutLastCalculatedCrc = calculateLatestMessagesCrc[F](latestMessagesList.init)
        withoutLastCalculatedCrc.value.map { withoutLastCalculatedCrcValue =>
          if (withoutLastCalculatedCrcValue == readLatestMessagesCrc) {
            latestMessagesRaf.setLength(latestMessagesRaf.length() - 64)
            (latestMessagesList.init.toMap, withoutLastCalculatedCrc)
          } else {
            // TODO: Restore latest messages from the persisted DAG
            latestMessagesRaf.setLength(0)
            (Map.empty[Validator, BlockHash], Crc32.empty[F]())
          }
        }
      }
    }
  }

  private def calculateDataLookupCrc[F[_]: Monad](
      dataLookupList: List[(BlockHash, BlockMetadata)]
  ): Crc32[F] =
    Crc32[F](
      dataLookupList
        .foldLeft(ByteString.EMPTY) {
          case (byteString, (_, blockMetadata)) =>
            val blockBytes = blockMetadata.toByteString
            byteString.concat(blockBytes.size().toByteString.concat(blockBytes))
        }
        .toByteArray
    )

  private def readDataLookupData[F[_]: Sync](
      dataLookupDataInput: DataInput
  ): F[List[(BlockHash, BlockMetadata)]] = {
    def readRec(
        result: List[(BlockHash, BlockMetadata)]
    ): F[List[(BlockHash, BlockMetadata)]] =
      for {
        blockSizeEither <- Sync[F].delay { dataLookupDataInput.readInt() }.attempt
        result <- blockSizeEither match {
                   case Right(blockSize) =>
                     val blockMetaBytes = Array.ofDim[Byte](blockSize)
                     for {
                       _             <- Sync[F].delay { dataLookupDataInput.readFully(blockMetaBytes) }
                       blockMetadata <- Sync[F].delay { BlockMetadata.fromBytes(blockMetaBytes) }
                       result        <- readRec((blockMetadata.blockHash -> blockMetadata) :: result)
                     } yield result
                   case Left(_: EOFException) =>
                     result.reverse.pure[F]
                   case Left(exception) =>
                     Sync[F].raiseError(exception)
                 }
      } yield result
    readRec(List.empty)
  }

  private def validateDataLookupData[F[_]: Monad](
      dataLookupRandomAccessFile: RandomAccessFile,
      readDataLookupCrc: Long,
      dataLookupCrcPath: Path,
      dataLookupList: List[(BlockHash, BlockMetadata)]
  ): F[(List[(BlockHash, BlockMetadata)], Crc32[F])] = {
    val fullCalculatedCrc = calculateDataLookupCrc[F](dataLookupList)
    fullCalculatedCrc.value.flatMap { fullCalculatedCrcValue =>
      if (fullCalculatedCrcValue == readDataLookupCrc) {
        (dataLookupList, fullCalculatedCrc).pure[F]
      } else if (dataLookupList.nonEmpty) {
        val withoutLastCalculatedCrc = calculateDataLookupCrc[F](dataLookupList.init)
        withoutLastCalculatedCrc.value.map { withoutLastCalculatedCrcValue =>
          if (withoutLastCalculatedCrcValue == readDataLookupCrc) {
            val byteString                    = dataLookupList.last._2.toByteString
            val lastDataLookupEntrySize: Long = 4L + byteString.size()
            dataLookupRandomAccessFile.setLength(
              dataLookupRandomAccessFile.length() - lastDataLookupEntrySize
            )
            (dataLookupList.init, withoutLastCalculatedCrc)
          } else {
            // TODO: Restore data lookup from block storage
            dataLookupRandomAccessFile.setLength(0)
            (List.empty[(BlockHash, BlockMetadata)], Crc32.empty[F]())
          }
        }
      } else {
        // TODO: Restore data lookup from block storage
        dataLookupRandomAccessFile.setLength(0)
        (List.empty[(BlockHash, BlockMetadata)], Crc32.empty[F]()).pure[F]
      }
    }
  }

  private def extractChildMap(
      dataLookup: List[(BlockHash, BlockMetadata)]
  ): Map[BlockHash, Set[BlockHash]] =
    dataLookup.foldLeft(dataLookup.map(_._1 -> Set.empty[BlockHash]).toMap) {
      case (childMap, (_, blockMetadata)) =>
        blockMetadata.parents.foldLeft(childMap) {
          case (acc, p) =>
            val currentChildren = acc.getOrElse(p, Set.empty[BlockHash])
            acc.updated(p, currentChildren + blockMetadata.blockHash)
        }
    }

  private def extractTopoSort(
      dataLookup: List[(BlockHash, BlockMetadata)]
  ): Vector[Vector[BlockHash]] = {
    val blockMetadatas = dataLookup.map(_._2).toVector
    val indexedTopoSort =
      blockMetadatas.groupBy(_.blockNum).mapValues(_.map(_.blockHash)).toVector.sortBy(_._1)
    assert(indexedTopoSort.zipWithIndex.forall { case ((readI, _), i) => readI == i })
    indexedTopoSort.map(_._2)
  }

  private def loadCheckpoints[F[_]: Sync: Log](checkpointsDirPath: Path): F[List[Checkpoint]] =
    for {
      files <- Sync[F].delay {
                checkpointsDirPath.toFile.mkdir()
                Files.list(checkpointsDirPath).filter(p => Files.isRegularFile(p))
              }
      filesList = files.collect(Collectors.toList[Path]).asScala.toList
      checkpoints <- filesList.flatTraverse { filePath =>
                      filePath.getFileName.toString match {
                        case checkpointPattern(start, end) =>
                          List(Checkpoint(start.toLong, end.toLong, filePath, None)).pure[F]
                        case other =>
                          Log[F].warn(s"Ignoring file '$other': not a valid checkpoint name") *>
                            List.empty[Checkpoint].pure[F]
                      }
                    }
      sortedCheckpoints = checkpoints.sortBy(_.start)
      result <- if (sortedCheckpoints.headOption.forall(_.start == 0)) {
                 if (sortedCheckpoints.isEmpty ||
                     sortedCheckpoints.zip(sortedCheckpoints.tail).forall {
                       case (current, next) => current.end == next.start
                     }) {
                   sortedCheckpoints.pure[F]
                 } else {
                   Sync[F].raiseError(CheckpointsAreNotConsecutive(sortedCheckpoints.map(_.path)))
                 }
               } else {
                 Sync[F].raiseError(CheckpointsDoNotStartFromZero(sortedCheckpoints.map(_.path)))
               }
    } yield result

  def create[F[_]: Concurrent: Sync: Capture: Log: BlockStore](
      config: Config
  ): F[BlockDagFileStorage[F]] =
    for {
      lock                  <- Semaphore[F](1)
      readLatestMessagesCrc <- readCrc[F](config.latestMessagesCrcPath)
      latestMessagesFileResource = Resource.fromAutoCloseable(
        Sync[F].delay { new RandomAccessFile(config.latestMessagesLogPath.toFile, "rw") }
      )
      latestMessagesResult <- latestMessagesFileResource.use { latestMessagesFile =>
                               for {
                                 latestMessagesReadResult <- readLatestMessagesData(
                                                              latestMessagesFile
                                                            )
                                 (latestMessagesList, logSize) = latestMessagesReadResult
                                 result <- validateLatestMessagesData[F](
                                            latestMessagesFile,
                                            readLatestMessagesCrc,
                                            config.latestMessagesCrcPath,
                                            latestMessagesList
                                          )
                                 (latestMessagesMap, calculatedLatestMessagesCrc) = result
                               } yield (latestMessagesMap, calculatedLatestMessagesCrc, logSize)
                             }
      (latestMessagesMap, calculatedLatestMessagesCrc, logSize) = latestMessagesResult
      readDataLookupCrc                                         <- readCrc[F](config.blockMetadataCrcPath)
      dataLookupFileResource = Resource.fromAutoCloseable(
        Sync[F].delay { new RandomAccessFile(config.blockMetadataLogPath.toFile, "rw") }
      )
      dataLookupResult <- dataLookupFileResource.use { dataLookupFile =>
                           for {
                             dataLookupList <- readDataLookupData(dataLookupFile)
                             result <- validateDataLookupData[F](
                                        dataLookupFile,
                                        readDataLookupCrc,
                                        config.blockMetadataCrcPath,
                                        dataLookupList
                                      )
                           } yield result
                         }
      (dataLookupList, calculatedDataLookupCrc) = dataLookupResult
      childMap                                  = extractChildMap(dataLookupList)
      topoSort                                  = extractTopoSort(dataLookupList)
      sortedCheckpoints                         <- loadCheckpoints(config.checkpointsDirPath)
      state = BlockDagFileStorageState(
        latestMessagesMap,
        childMap,
        dataLookupList.toMap,
        topoSort,
        sortedCheckpoints.lastOption.map(_.end).getOrElse(0L),
        sortedCheckpoints,
        new FileOutputStream(
          config.latestMessagesLogPath.toFile,
          true
        ),
        logSize,
        calculatedLatestMessagesCrc,
        new FileOutputStream(
          config.blockMetadataLogPath.toFile,
          true
        ),
        calculatedDataLookupCrc
      )
    } yield
      new BlockDagFileStorage[F](
        lock,
        config.latestMessagesLogPath,
        config.latestMessagesCrcPath,
        config.latestMessagesLogMaxSizeFactor,
        config.blockMetadataLogPath,
        config.blockMetadataCrcPath,
        new AtomicMonadState[F, BlockDagFileStorageState[F]](AtomicAny(state))
      )

  def createEmptyFromGenesis[F[_]: Concurrent: Sync: Capture: Log: BlockStore](
      config: Config,
      genesis: BlockMessage
  ): F[BlockDagFileStorage[F]] =
    for {
      lock                  <- Semaphore[F](1)
      _                     <- Sync[F].delay { Files.createFile(config.latestMessagesLogPath) }
      _                     <- Sync[F].delay { Files.createFile(config.latestMessagesCrcPath) }
      genesisBonds          = BlockMessageUtil.bonds(genesis)
      initialLatestMessages = genesisBonds.map(_.validator -> genesis.blockHash).toMap
      latestMessagesData = initialLatestMessages
        .foldLeft(ByteString.EMPTY) {
          case (byteString, (validator, blockHash)) =>
            byteString.concat(validator).concat(blockHash)
        }
        .toByteArray
      latestMessagesCrc = Crc32.empty[F]()
      _ <- initialLatestMessages.toList.traverse_ {
            case (validator, blockHash) =>
              latestMessagesCrc.update(validator.concat(blockHash).toByteArray)
          }
      latestMessagesCrcBytes <- latestMessagesCrc.bytes
      _                      <- Sync[F].delay { Files.write(config.latestMessagesLogPath, latestMessagesData) }
      _                      <- Sync[F].delay { Files.write(config.latestMessagesCrcPath, latestMessagesCrcBytes) }
      blockMetadataCrc       = Crc32.empty[F]()
      genesisByteString      = genesis.toByteString
      genesisData            = genesisByteString.size.toByteString.concat(genesisByteString).toByteArray
      _                      <- blockMetadataCrc.update(genesisData)
      blockMetadataCrcBytes  <- blockMetadataCrc.bytes
      _                      <- Sync[F].delay { Files.write(config.blockMetadataLogPath, genesisData) }
      _                      <- Sync[F].delay { Files.write(config.blockMetadataCrcPath, blockMetadataCrcBytes) }
      state = BlockDagFileStorageState(
        initialLatestMessages,
        Map(genesis.blockHash -> Set.empty[BlockHash]),
        Map(genesis.blockHash -> BlockMetadata.fromBlock(genesis)),
        Vector(Vector(genesis.blockHash)),
        0L,
        List.empty,
        new FileOutputStream(
          config.latestMessagesLogPath.toFile,
          true
        ),
        initialLatestMessages.size,
        latestMessagesCrc,
        new FileOutputStream(
          config.blockMetadataLogPath.toFile,
          true
        ),
        blockMetadataCrc
      )
    } yield
      new BlockDagFileStorage[F](
        lock,
        config.latestMessagesLogPath,
        config.latestMessagesCrcPath,
        config.latestMessagesLogMaxSizeFactor,
        config.blockMetadataLogPath,
        config.blockMetadataCrcPath,
        new AtomicMonadState[F, BlockDagFileStorageState[F]](AtomicAny(state))
      )
}
