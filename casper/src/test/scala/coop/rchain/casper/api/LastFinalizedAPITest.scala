package coop.rchain.casper.api

import cats.effect.Sync
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.casper.engine.Engine
import coop.rchain.casper.{EngineWithCasper, SafetyOracle}
import coop.rchain.casper.helper._
import coop.rchain.casper.helper.BlockGenerator._
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.GenesisBuilder._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.ConstructDeploy.basicDeployData
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.metrics.Metrics
import coop.rchain.shared.Cell
import coop.rchain.shared.scalatestcontrib.effectTest
import monix.eval.Task
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import monix.execution.Scheduler.Implicits.global

import scala.collection.immutable.Map

class LastFinalizedAPITest
    extends FlatSpec
    with Matchers
    with EitherValues
    with BlockGenerator
    with BlockDagStorageFixture {
  val validatorKeyPairs = (1 to 3).map(_ => Secp256k1.newKeyPair)
  val (_, validatorPks) = validatorKeyPairs.unzip
  val validators        = validatorPks.map(pk => ByteString.copyFrom(pk.bytes))
  val v1                = validators(0)
  val v2                = validators(1)
  val v3                = validators(2)
  val genesisParameters = buildGenesisParameters(
    validatorKeyPairs,
    Map(validatorPks(0) -> 50, validatorPks(1) -> 45, validatorPks(2) -> 5)
  )
  val genesisContext = buildGenesis(genesisParameters)

  implicit val metricsEff = new Metrics.MetricsNOP[Task]

  /*
   * DAG Looks like this:
   *
   *           b5
   *           |
   *           b4
   *           |
   *           b3
   *           |
   *           b2 <- last finalized block
   *           |
   *           b1
   *           |
   *         genesis
   */
  "isFinalized" should "return true for ancestors of last finalized block" in effectTest {
    HashSetCasperTestNode.networkEff(genesisContext, networkSize = 3).use {
      case n1 +: n2 +: n3 +: _ =>
        import n1.{blockStore, cliqueOracleEffect, logEff}
        val engine = new EngineWithCasper[Task](n1.casperEff)
        for {
          produceDeploys <- (0 until 5).toList.traverse(i => basicDeployData[Task](i))

          b1 <- n1.addBlock(produceDeploys(0))
          _  <- n2.receive()
          _  <- n3.receive()

          b2 <- n2.addBlock(produceDeploys(1))
          _  <- n1.receive()
          _  <- n3.receive()

          b3 <- n3.addBlock(produceDeploys(2))
          _  <- n1.receive()
          _  <- n2.receive()

          b4 <- n1.addBlock(produceDeploys(3))
          _  <- n2.receive()
          _  <- n3.receive()

          b5 <- n2.addBlock(produceDeploys(4))
          _  <- n1.receive()
          _  <- n3.receive()

          lastFinalizedBlock <- n1.casperEff.lastFinalizedBlock
          _                  = lastFinalizedBlock shouldBe b2

          engineCell <- Cell.mvarCell[Task, Engine[Task]](engine)
          // Checking if last finalized block is finalized
          isB2Finalized <- BlockAPI.isFinalized[Task](
                            IsFinalizedQuery(hash = ProtoUtil.hashString(b2))
                          )(Sync[Task], engineCell, SafetyOracle[Task], blockStore, logEff)
          _ = isB2Finalized.right.value shouldBe IsFinalizedResponse(isFinalized = true)
          // Checking if parent of last finalized block is finalized
          isB1Finalized <- BlockAPI.isFinalized[Task](
                            IsFinalizedQuery(hash = ProtoUtil.hashString(b1))
                          )(Sync[Task], engineCell, SafetyOracle[Task], blockStore, logEff)
          _ = isB1Finalized.right.value shouldBe IsFinalizedResponse(isFinalized = true)
        } yield ()
    }
  }

  /*
   * DAG Looks like this:
   *
   *           b5
   *           |
   *           b4
   *           |
   *       b2  b3 <- last finalized block
   *         \ |
   *           b1
   *           |
   *         genesis
   */
  it should "return false for children and siblings of last finalized block" in effectTest {
    HashSetCasperTestNode.networkEff(genesisContext, networkSize = 3).use {
      case n1 +: n2 +: n3 +: _ =>
        import n3.{blockStore, cliqueOracleEffect, logEff}
        val engine = new EngineWithCasper[Task](n3.casperEff)
        for {
          produceDeploys <- (0 until 5).toList.traverse(i => basicDeployData[Task](i))

          b1 <- n1.addBlock(produceDeploys(0))
          _  <- n2.receive()
          _  <- n3.receive()

          b2 <- n3.addBlock(produceDeploys(1))

          b3 <- n2.addBlock(produceDeploys(2))
          _  <- n1.receive()
          _  <- n3.receive()

          b4 <- n1.addBlock(produceDeploys(3))
          _  <- n2.receive()
          _  <- n3.receive()

          b5 <- n2.addBlock(produceDeploys(4))
          _  <- n1.receive()
          _  <- n3.receive()

          lastFinalizedBlock <- n3.casperEff.lastFinalizedBlock
          _                  = lastFinalizedBlock shouldBe b3

          engineCell <- Cell.mvarCell[Task, Engine[Task]](engine)
          // Checking if child of last finalized block is finalized
          isB2Finalized <- BlockAPI.isFinalized[Task](
                            IsFinalizedQuery(hash = ProtoUtil.hashString(b4))
                          )(Sync[Task], engineCell, SafetyOracle[Task], blockStore, logEff)
          _ = isB2Finalized.right.value shouldBe IsFinalizedResponse(isFinalized = false)
          // Checking if sibling of last finalized block is finalized
          isB1Finalized <- BlockAPI.isFinalized[Task](
                            IsFinalizedQuery(hash = ProtoUtil.hashString(b2))
                          )(Sync[Task], engineCell, SafetyOracle[Task], blockStore, logEff)
          _ = isB1Finalized.right.value shouldBe IsFinalizedResponse(isFinalized = false)
        } yield ()
    }
  }
}
