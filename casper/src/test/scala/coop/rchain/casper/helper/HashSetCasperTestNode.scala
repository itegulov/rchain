package coop.rchain.casper.helper

import java.nio.file.{Files, Path}

import cats.data.EitherT
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import cats.{Applicative, ApplicativeError, Id, Monad}
import coop.rchain.blockstorage._
import coop.rchain.casper._
import coop.rchain.casper.helper.BlockDagStorageTestFixture.mapSize
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.comm.CasperPacketHandler.{
  ApprovedBlockReceivedHandler,
  CasperPacketHandlerImpl,
  CasperPacketHandlerInternal
}
import coop.rchain.casper.util.comm.TestNetwork.TestNetwork
import coop.rchain.casper.util.comm._
import coop.rchain.casper.util.rholang.{InterpreterUtil, RuntimeManager}
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.catscontrib.effect.implicits._
import coop.rchain.catscontrib.ski._
import coop.rchain.comm.CommError.ErrorHandler
import coop.rchain.comm._
import coop.rchain.comm.protocol.routing._
import coop.rchain.comm.rp.Connect
import coop.rchain.comm.rp.Connect._
import coop.rchain.comm.rp.HandleMessages.handle
import coop.rchain.crypto.PrivateKey
import coop.rchain.crypto.signatures.Ed25519
import coop.rchain.metrics
import coop.rchain.metrics.{Metrics, NoopSpan}
import coop.rchain.p2p.EffectsTestInstances._
import coop.rchain.p2p.effects.PacketHandler
import coop.rchain.rholang.interpreter.Runtime
import coop.rchain.rspace.Context
import coop.rchain.shared.PathOps.RichPath
import coop.rchain.shared._
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.util.Random

class HashSetCasperTestNode[F[_]](
    name: String,
    val local: PeerNode,
    tle: TransportLayerTestImpl[F],
    tls: TransportLayerServerTestImpl[F],
    val genesis: BlockMessage,
    sk: PrivateKey,
    logicalTime: LogicalTime[F],
    implicit val errorHandlerEff: ErrorHandler[F],
    val blockDagDir: Path,
    val blockStoreDir: Path,
    blockProcessingLock: Semaphore[F],
    shardId: String = "rchain",
    val runtimeManager: RuntimeManager[F]
)(
    implicit concurrentF: Concurrent[F],
    val blockStore: BlockStore[F],
    val blockDagStorage: BlockDagStorage[F],
    val metricEff: Metrics[F],
    val casperState: Cell[F, CasperState]
) {

  implicit val logEff             = new LogStub[F]
  implicit val timeEff            = logicalTime
  implicit val connectionsCell    = Cell.unsafe[F, Connections](Connect.Connections.empty)
  implicit val transportLayerEff  = tle
  implicit val cliqueOracleEffect = SafetyOracle.cliqueOracle[F]
  implicit val rpConfAsk          = createRPConfAsk[F](local)

  val defaultTimeout: FiniteDuration = FiniteDuration(1000, MILLISECONDS)

  val validatorId = ValidatorIdentity(Ed25519.toPublic(sk), sk, "ed25519")

  val approvedBlock = ApprovedBlock(candidate = Some(ApprovedBlockCandidate(block = Some(genesis))))

  implicit val labF        = LastApprovedBlock.unsafe[F](Some(approvedBlock))
  val postGenesisStateHash = ProtoUtil.postStateHash(genesis)

  implicit val casperEff = new MultiParentCasperImpl[F](
    runtimeManager,
    Some(validatorId),
    genesis,
    postGenesisStateHash,
    shardId,
    blockProcessingLock
  )

  implicit val multiparentCasperRef = MultiParentCasperRef.unsafe[F](Some(casperEff))

  val handlerInternal = new ApprovedBlockReceivedHandler(casperEff, approvedBlock)
  val casperPacketHandler =
    new CasperPacketHandlerImpl[F](Ref.unsafe[F, CasperPacketHandlerInternal[F]](handlerInternal))
  implicit val packetHandlerEff = PacketHandler.pf[F](
    casperPacketHandler.handle
  )

  val span = new NoopSpan[F]

  def initialize(): F[Unit] =
    // pre-population removed from internals of Casper
    blockStore.put(genesis.blockHash, genesis) *>
      blockDagStorage.getRepresentation.flatMap { dag =>
        InterpreterUtil
          .validateBlockCheckpoint[F](
            genesis,
            dag,
            runtimeManager,
            span
          )
          .void
      }

  def receive(): F[Unit] = tls.receive(p => handle[F](p), kp(().pure[F])).void
}

object HashSetCasperTestNode {
  type CommErrT[F[_], A] = EitherT[F, CommError, A]
  type Effect[A]         = CommErrT[Task, A]

  import coop.rchain.catscontrib._

  def createRuntime(storageDirectory: Path, storageSize: Long)(
      implicit scheduler: Scheduler
  ): Resource[Effect, RuntimeManager[Effect]] = {
    implicit val log                       = new Log.NOPLog[Task]()
    implicit val metricsEff: Metrics[Task] = new metrics.Metrics.MetricsNOP[Task]
    val activeRuntime =
      Runtime
        .createWithEmptyCost[Task, Task.Par](storageDirectory, storageSize, StoreType.LMDB)
        .unsafeRunSync
    val runtimeManager = RuntimeManager.fromRuntime(activeRuntime).unsafeRunSync
    Resource.make[Effect, RuntimeManager[Effect]](
      RuntimeManager.eitherTRuntimeManager[CommError, Task](runtimeManager).pure[Effect]
    )(_ => activeRuntime.close().liftM[CommErrT])
  }

  def rigConnectionsF[F[_]: Monad](
      n: HashSetCasperTestNode[F],
      nodes: List[HashSetCasperTestNode[F]]
  ): F[List[HashSetCasperTestNode[F]]] = {
    import Connections._
    for {
      _ <- nodes.traverse(
            m =>
              n.connectionsCell
                .flatModify(_.addConn[F](m.local))
          )
      _ <- nodes.traverse(
            m => m.connectionsCell.flatModify(_.addConn[F](n.local))
          )
    } yield nodes ++ (n :: Nil)
  }

  def standaloneF[F[_]](
      genesis: BlockMessage,
      sk: PrivateKey,
      blockDagDir: Path,
      blockStoreDir: Path,
      storageSize: Long,
      createRuntime: (Path, Long) => Resource[F, RuntimeManager[F]]
  )(
      implicit errorHandler: ErrorHandler[F],
      concurrentF: Concurrent[F],
      testNetworkF: TestNetwork[F]
  ): Resource[F, HashSetCasperTestNode[F]] = {
    val name                        = "standalone"
    val identity                    = peerNode(name, 40400)
    val tle                         = new TransportLayerTestImpl[F]()
    val tls                         = new TransportLayerServerTestImpl[F](identity)
    val logicalTime: LogicalTime[F] = new LogicalTime[F]
    implicit val log                = new Log.NOPLog[F]()
    implicit val metricEff          = new Metrics.MetricsNOP[F]
    val env                         = Context.env(blockStoreDir, mapSize)
    for {
      blockStore <- FileLMDBIndexBlockStore.createUnsafe[F](env, blockStoreDir)
      blockDagStorage <- Resource.make[F, BlockDagStorage[F]](
                          BlockDagFileStorage
                            .createEmptyFromGenesis[F](
                              BlockDagFileStorage.Config(
                                blockDagDir.resolve("latest-messages-data"),
                                blockDagDir.resolve("latest-messages-crc"),
                                blockDagDir.resolve("block-metadata-data"),
                                blockDagDir.resolve("block-metadata-crc"),
                                blockDagDir.resolve("equivocations-tracker-data"),
                                blockDagDir.resolve("equivocations-tracker-crc"),
                                blockDagDir.resolve("invalid-blocks-data"),
                                blockDagDir.resolve("invalid-blocks-crc"),
                                blockDagDir.resolve("checkpoints"),
                                blockDagDir.resolve("block-number-index"),
                                mapSize
                              ),
                              genesis
                            )(Concurrent[F], Sync[F], Log[F])
                            .widen[BlockDagStorage[F]]
                        )(_.close())
      storageDirectory <- Resource.make[F, Path](
                           Sync[F].delay {
                             Files.createTempDirectory(s"hash-set-casper-test-$name")
                           }
                         )(dir => Sync[F].delay { dir.recursivelyDelete() })
      runtimeManager <- createRuntime(storageDirectory, storageSize)
      node <- Resource.make[F, HashSetCasperTestNode[F]] {
               for {
                 _                   <- TestNetwork.addPeer(identity)
                 blockProcessingLock <- Semaphore[F](1)
                 casperState         <- Cell.mvarCell[F, CasperState](CasperState())
                 node = new HashSetCasperTestNode[F](
                   name,
                   identity,
                   tle,
                   tls,
                   genesis,
                   sk,
                   logicalTime,
                   errorHandler,
                   blockDagDir,
                   blockStoreDir,
                   blockProcessingLock,
                   "rchain",
                   runtimeManager
                 )(
                   concurrentF,
                   blockStore,
                   blockDagStorage,
                   metricEff,
                   casperState
                 )
                 result <- node.initialize().map(_ => node)
               } yield result
             }(_ => ().pure[F])
    } yield node
  }
  def standaloneEff(
      genesis: BlockMessage,
      sk: PrivateKey,
      storageSize: Long = 1024L * 1024 * 10,
      testNetwork: TestNetwork[Effect] = TestNetwork.empty
  )(
      implicit scheduler: Scheduler
  ): Resource[Effect, HashSetCasperTestNode[Effect]] =
    BlockDagStorageTestFixture.createDirectories[Effect].flatMap {
      case (blockStoreDir, blockDagDir) =>
        standaloneF[Effect](
          genesis,
          sk,
          blockDagDir,
          blockStoreDir,
          storageSize,
          createRuntime
        )(
          ApplicativeError_[Effect, CommError],
          Concurrent[Effect],
          testNetwork
        )
    }

  def networkF[F[_]](
      sks: IndexedSeq[PrivateKey],
      genesis: BlockMessage,
      storageSize: Long,
      createRuntime: (Path, Long) => Resource[F, RuntimeManager[F]]
  )(
      implicit errorHandler: ErrorHandler[F],
      concurrentF: Concurrent[F],
      testNetworkF: TestNetwork[F]
  ): Resource[F, IndexedSeq[HashSetCasperTestNode[F]]] = {
    val n                           = sks.length
    val names                       = (1 to n).map(i => s"node-$i")
    val peers                       = names.map(peerNode(_, 40400))
    val logicalTime: LogicalTime[F] = new LogicalTime[F]

    val nodesF =
      names
        .zip(peers)
        .zip(sks)
        .toList
        .traverse {
          case ((n, p), sk) =>
            val tle                = new TransportLayerTestImpl[F]()
            val tls                = new TransportLayerServerTestImpl[F](p)
            implicit val log       = new Log.NOPLog[F]()
            implicit val metricEff = new Metrics.MetricsNOP[F]
            for {
              storageDirectories           <- BlockDagStorageTestFixture.createDirectories[F]
              (blockStoreDir, blockDagDir) = storageDirectories
              env                          = Context.env(blockStoreDir, mapSize)
              blockStore                   <- FileLMDBIndexBlockStore.createUnsafe[F](env, blockStoreDir)
              blockDagStorage <- Resource.make[F, BlockDagStorage[F]](
                                  BlockDagFileStorage
                                    .createEmptyFromGenesis[F](
                                      BlockDagFileStorage.Config(
                                        blockDagDir.resolve("latest-messages-data"),
                                        blockDagDir.resolve("latest-messages-crc"),
                                        blockDagDir.resolve("block-metadata-data"),
                                        blockDagDir.resolve("block-metadata-crc"),
                                        blockDagDir.resolve("equivocations-tracker-data"),
                                        blockDagDir.resolve("equivocations-tracker-crc"),
                                        blockDagDir.resolve("invalid-blocks-data"),
                                        blockDagDir.resolve("invalid-blocks-crc"),
                                        blockDagDir.resolve("checkpoints"),
                                        blockDagDir.resolve("block-number-index"),
                                        mapSize
                                      ),
                                      genesis
                                    )(Concurrent[F], Sync[F], Log[F])
                                    .widen
                                )(_.close())
              storageDirectory <- Resource.make[F, Path](
                                   Sync[F].delay {
                                     Files.createTempDirectory(s"hash-set-casper-test-$n")
                                   }
                                 )(dir => Sync[F].delay { dir.recursivelyDelete() })
              runtimeManager <- createRuntime(storageDirectory, storageSize)
              node <- Resource.make[F, HashSetCasperTestNode[F]](
                       for {
                         _         <- TestNetwork.addPeer(p)
                         semaphore <- Semaphore[F](1)
                         casperState <- Cell.mvarCell[F, CasperState](
                                         CasperState()
                                       )
                         node = new HashSetCasperTestNode[F](
                           n,
                           p,
                           tle,
                           tls,
                           genesis,
                           sk,
                           logicalTime,
                           errorHandler,
                           blockDagDir,
                           blockStoreDir,
                           semaphore,
                           "rchain",
                           runtimeManager
                         )(
                           concurrentF,
                           blockStore,
                           blockDagStorage,
                           metricEff,
                           casperState
                         )
                       } yield node
                     )(_ => ().pure[F])
            } yield node
        }
        .map(_.toVector)

    nodesF.flatMap { nodes =>
      import Connections._
      //make sure all nodes know about each other
      Resource.liftF(
        for {
          _ <- nodes.traverse_[F, Unit](_.initialize())
          pairs = for {
            n <- nodes
            m <- nodes
            if n.local != m.local
          } yield (n, m)
          _ <- pairs.foldLeft(().pure[F]) {
                case (f, (n, m)) =>
                  f.flatMap(
                    _ =>
                      n.connectionsCell.flatModify(
                        _.addConn[F](m.local)
                      )
                  )
              }
        } yield nodes
      )
    }
  }
  def networkEff(
      sks: IndexedSeq[PrivateKey],
      genesis: BlockMessage,
      storageSize: Long = 1024L * 1024 * 10,
      testNetwork: TestNetwork[Effect] = TestNetwork.empty
  )(implicit scheduler: Scheduler): Resource[Effect, IndexedSeq[HashSetCasperTestNode[Effect]]] =
    networkF[Effect](
      sks,
      genesis,
      storageSize,
      createRuntime
    )(
      ApplicativeError_[Effect, CommError],
      Concurrent[Effect],
      testNetwork
    )

  val appErrId = new ApplicativeError[Id, CommError] {
    def ap[A, B](ff: Id[A => B])(fa: Id[A]): Id[B] = Applicative[Id].ap[A, B](ff)(fa)
    def pure[A](x: A): Id[A]                       = Applicative[Id].pure[A](x)
    def raiseError[A](e: CommError): Id[A] = {
      val errString = e match {
        case UnknownCommError(msg)                => s"UnknownCommError($msg)"
        case DatagramSizeError(size)              => s"DatagramSizeError($size)"
        case DatagramFramingError(ex)             => s"DatagramFramingError($ex)"
        case DatagramException(ex)                => s"DatagramException($ex)"
        case HeaderNotAvailable                   => "HeaderNotAvailable"
        case ProtocolException(th)                => s"ProtocolException($th)"
        case UnknownProtocolError(msg)            => s"UnknownProtocolError($msg)"
        case PublicKeyNotAvailable(node)          => s"PublicKeyNotAvailable($node)"
        case ParseError(msg)                      => s"ParseError($msg)"
        case EncryptionHandshakeIncorrectlySigned => "EncryptionHandshakeIncorrectlySigned"
        case BootstrapNotProvided                 => "BootstrapNotProvided"
        case PeerNodeNotFound(peer)               => s"PeerNodeNotFound($peer)"
        case PeerUnavailable(peer)                => s"PeerUnavailable($peer)"
        case MalformedMessage(pm)                 => s"MalformedMessage($pm)"
        case CouldNotConnectToBootstrap           => "CouldNotConnectToBootstrap"
        case InternalCommunicationError(msg)      => s"InternalCommunicationError($msg)"
        case TimeOut                              => "TimeOut"
        case _                                    => e.toString
      }

      throw new Exception(errString)
    }

    def handleErrorWith[A](fa: Id[A])(f: (CommError) => Id[A]): Id[A] = fa
  }

  implicit val syncEffectInstance = cats.effect.Sync.catsEitherTSync[Task, CommError]

  val errorHandler = ApplicativeError_.applicativeError[Id, CommError](appErrId)

  def randomBytes(length: Int): Array[Byte] = Array.fill(length)(Random.nextInt(256).toByte)

  def endpoint(port: Int): Endpoint = Endpoint("host", port, port)

  def peerNode(name: String, port: Int): PeerNode =
    PeerNode(NodeIdentifier(name.getBytes), endpoint(port))

}
