package coop.rchain.casper

import scala.util.Random._

import cats.{Applicative, Functor, Monad}
import cats.implicits._
import coop.rchain.blockstorage.{BlockStore, IndexedBlockDagStorage}
import coop.rchain.casper.helper.HashSetCasperTestNode
import coop.rchain.casper.helper.HashSetCasperTestNode._
import coop.rchain.casper.protocol.DeployData
import coop.rchain.casper.scalatestcontrib._
import coop.rchain.casper.util.{ConstructDeploy, RSpaceUtil}
import coop.rchain.p2p.EffectsTestInstances.LogicalTime
import monix.execution.Scheduler.Implicits.global
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class MultiParentCasperMergeSpec extends FlatSpec with Matchers with Inspectors {

  import RSpaceUtil._
  import coop.rchain.casper.util.GenesisBuilder._

  implicit val timeEff = new LogicalTime[Effect]

  val genesis = buildGenesis()

  "HashSetCasper" should "handle multi-parent blocks correctly" in effectTest {
    HashSetCasperTestNode.networkEff(genesis, networkSize = 2).use { nodes =>
      implicit val rm = nodes(1).runtimeManager
      for {
        deployData0 <- ConstructDeploy.basicDeployData[Effect](0)
        deployData1 <- ConstructDeploy.sourceDeployNowF("@1!(1) | for(@x <- @1){ @1!(x) }")
        deployData2 <- ConstructDeploy.basicDeployData[Effect](2)
        deploys = Vector(
          deployData0,
          deployData1,
          deployData2
        )
        block0 <- nodes(0).addBlock(deploys(0))
        block1 <- nodes(1).addBlock(deploys(1))
        _      <- nodes(0).receive()
        _      <- nodes(1).receive()
        _      <- nodes(0).receive()
        _      <- nodes(1).receive()

        //multiparent block joining block0 and block1 since they do not conflict
        multiparentBlock <- nodes(0).addBlock(deploys(2))
        _                <- nodes(1).receive()

        _ = nodes(0).logEff.warns.isEmpty shouldBe true
        _ = nodes(1).logEff.warns.isEmpty shouldBe true
        _ = multiparentBlock.header.get.parentsHashList.size shouldBe 2
        _ = nodes(0).casperEff.contains(multiparentBlock.blockHash) shouldBeF true
        _ = nodes(1).casperEff.contains(multiparentBlock.blockHash) shouldBeF true
        _ <- getDataAtPublicChannel[Effect](multiparentBlock, 0).map(_ shouldBe Seq("0"))
        _ <- getDataAtPublicChannel[Effect](multiparentBlock, 1).map(_ shouldBe Seq("1"))
        _ <- getDataAtPublicChannel[Effect](multiparentBlock, 2).map(_ shouldBe Seq("2"))
      } yield ()
    }
  }

  it should "handle multi-parent blocks correctly when they operate on stdout" in effectTest {
    def echoContract(no: Int) =
      Rho(s"""new stdout(`rho:io:stdout`) in { stdout!("Contract $no") }""")
    merges(echoContract(1), echoContract(2), Rho("Nil"))
  }

  trait MergeableCase {
    def apply(left: Rho*)(right: Rho*)(base: Rho*): Effect[Unit] =
      merges(left.reduce(_ | _), right.reduce(_ | _), base.reduce(_ | _))
  }

  trait ConflictingCase {
    def apply(left: Rho*)(right: Rho*)(base: Rho*): Effect[Unit] =
      conflicts(left.reduce(_ | _), right.reduce(_ | _), base.reduce(_ | _))
  }

  /***
   Two incoming sends/receives, at most one had a matching dual in TS.
   The incoming events won't cause more COMMs together (same polarity).
   They couldn't be competing for the same linear receive/send (at most one had a match).
   Notice this includes "two unsatisfied" and "must be looking for different data" cases.
    */
  object SamePolarityMerge extends MergeableCase

  /***
   Two incoming sends/receives each matched a receive/send that was in TS.
   The incoming events won't cause more COMMs together (same polarity).
   They could've matched the same linear event.
   Mergeable if different events, or at least one matched event is non-linear.
   This is the case where both incoming events could have matched what was in the other TS.
    */
  object CouldMatchSameConflicts extends ConflictingCase

  /***
   Two incoming sends/receives each matched a receive/send that was in TS.
   The incoming events won't cause more COMMs together (same polarity).
   They could've matched the same linear event.
   Mergeable if different events, or at least one matched event is non-linear.
   This is the case where the incoming events match differently
    */
  object CouldMatchSameMerges extends MergeableCase

  /***
   A send and a receive were incoming, at least one had a match, either:
   - both were linear
   - one was non-linear, the other had a match
   They couldn't match the same linear event (they have different polarity)
   They couldn't spawn more work, because either:
   - both were linear, one of them had a match in TS
   - one was non-linear, but the other chose to go with its match
    */
  object HadItsMatch extends MergeableCase

  /***
   An incoming send and an incoming receive could match each other,
   leading to more COMMs needing to happen.
   Mergeable if we use spatial matcher to prove they don't match.
    */
  object IncomingCouldMatch extends ConflictingCase

  /***
   There was a COMM within one of the deploys.
   The other deploy saw none of it.
    */
  object VolatileEvent extends MergeableCase

  /***
   There was a COMM within one of the deploys, with one side non-linear.
   The other deploy had an event without a match in TS, dual to the non-linear.
   These could spawn more work.
   Mergeable if we use spatial matcher to prove they don't match.
    */
  object PersistentCouldMatch extends ConflictingCase

  /***
   There was a COMM within one of the deploys, with one side non-linear.
   The other deploy had an event without a match in TS, of same polarity to the non-linear.
   These could not spawn more work.
    */
  object PersistentNoMatch extends MergeableCase

  case class Rho(
      value: String,
      maybePolarity: Option[Polarity] = None,
      maybeCardinality: Option[Cardinality] = None
  ) {
    def |(other: Rho): Rho = Rho(s"$value | ${other.value}")
  }
  object Nil extends Rho("Nil")

  // Sends (linear sends)
  val S0 = Rho("@0!(0)", Some(Send), Some(Linear))
  val S1 = Rho("@0!(1)", Some(Send), Some(Linear))
  // Repeats (persistent sends)
  val R0 = Rho("@0!!(0)", Some(Send), Some(NonLinear))
  val R1 = Rho("@0!!(1)", Some(Send), Some(NonLinear))
  // For-s (linear receives)
  val F_ = Rho("for (_ <- @0) { 0 }", Some(Receive), Some(Linear))
  val F0 = Rho("for (@0 <- @0) { 0 }", Some(Receive), Some(Linear))
  val F1 = Rho("for (@1 <- @0) { 0 }", Some(Receive), Some(Linear))
  // Contracts (persistent receives)
  val C_ = Rho("contract @0(id) = { 0 }", Some(Receive), Some(NonLinear))
  val C0 = Rho("contract @0(@0) = { 0 }", Some(Receive), Some(NonLinear))
  val C1 = Rho("contract @0(@1) = { 0 }", Some(Receive), Some(NonLinear))

  // TODO: Peek rows/column
  // Note this skips pairs that lead to infinite loops
  val mergeabilityCases = List(
    "!X !X"     -> SamePolarityMerge(S0)(S0)(Nil),
    "!X !4"     -> SamePolarityMerge(S0)(S1)(F1),
    "!X (!4)"   -> VolatileEvent(S0)(S0 | F_)(Nil),
    "!X !C"     -> SamePolarityMerge(S0)(S1)(C1),
    "!X (!C)"   -> PersistentCouldMatch(S0)(S0 | C_)(Nil),
    "!X 4X"     -> IncomingCouldMatch(S0)(F_)(Nil),
    "!X 4!"     -> HadItsMatch(S0)(F_)(S0),
    "!X (4!)"   -> coveredBy("!X (!4)"),
    "!X 4!!"    -> HadItsMatch(S0)(F_)(R0),
    "!X (4!!)"  -> PersistentNoMatch(S0)(F_ | R0)(Nil),
    "!X !!X"    -> SamePolarityMerge(S0)(R0)(Nil),
    "!X !!4"    -> SamePolarityMerge(S0)(R1)(F1),
    "!X (!!4)"  -> coveredBy("!X (4!!)"),
    "!X CX"     -> IncomingCouldMatch(S0)(C_)(Nil),
    "!X C!"     -> IncomingCouldMatch(S0)(C_)(S0),
    "!X (C!)"   -> PersistentCouldMatch(S0)(C_ | S0)(Nil),
    "!4 !4"     -> CouldMatchSameConflicts(S0)(S1)(F_),
    "!4 !4"     -> CouldMatchSameMerges(S0)(S1)(F0 | F1),
    "!4 (!4)"   -> VolatileEvent(S0)(S1 | F_)(F0),
    "(!4) (!4)" -> VolatileEvent(S0 | F_)(S0 | F_)(Nil),
    "!4 !C"     -> CouldMatchSameMerges(S0)(S1)(F0 | C1),
    "!4 4X"     -> HadItsMatch(S0)(F_)(F_),
    "!4 4!"     -> HadItsMatch(S0)(F_)(F0 | S1),
    "!4 (4!)"   -> VolatileEvent(S0)(S1 | F_)(F0),
    "!4 4!!"    -> HadItsMatch(S0)(F_)(F0 | R1),
    "!4 !!X"    -> SamePolarityMerge(S0)(R1)(F0),
    "!4 !!4"    -> CouldMatchSameConflicts(S0)(R1)(F_),
    "!4 !!4"    -> CouldMatchSameMerges(S0)(R1)(F0 | F1),
    "!4 CX"     -> HadItsMatch(S0)(C_)(F_),
    "!4 C!"     -> HadItsMatch(S0)(C_)(F0 | S1),
    "!4 (C!)"   -> PersistentNoMatch(S0)(C_ | S1)(F0),
    "!4 (!C)"   -> PersistentNoMatch(S0)(S1 | C_)(F0),
    "!C !C"     -> CouldMatchSameMerges(S0)(S0)(C_),
    "!C (!C)"   -> PersistentNoMatch(S0)(C_ | S1)(C0),
    "(!C) !C"   -> coveredBy("!C (!C)"),
    "!C 4X"     -> HadItsMatch(S0)(F_)(C_),
    "!C 4!"     -> HadItsMatch(S0)(F_)(C0 | S1),
    "!C (4!)"   -> VolatileEvent(S0)(F1 | S1)(C0),
    "!C 4!!"    -> HadItsMatch(S0)(F_)(C0 | R1),
    "!C !!X"    -> SamePolarityMerge(S0)(R1)(C0),
    "!C !!4"    -> CouldMatchSameMerges(S0)(R1)(C0 | F1),
    "!C CX"     -> HadItsMatch(S0)(C_)(C_),
    "!C C!"     -> HadItsMatch(S0)(C_)(C0 | S1),
    "!C (C!)"   -> coveredBy("!C (!C)"),
    "4X 4X"     -> SamePolarityMerge(F_)(F_)(Nil),
    "4X 4!"     -> SamePolarityMerge(F0)(F_)(S1),
    "4X CX"     -> SamePolarityMerge(F_)(C_)(Nil),
    "4X C!"     -> SamePolarityMerge(F0)(C1)(S1),
    "4X (!!4)"  -> PersistentCouldMatch(F_)(R0 | F_)(Nil),
    "4! 4!"     -> CouldMatchSameConflicts(F_)(F_)(S0),
    "4! 4!"     -> CouldMatchSameMerges(F0)(F1)(S0 | S1),
    "4! CX"     -> SamePolarityMerge(F_)(C1)(S0),
    "4! C!"     -> CouldMatchSameConflicts(F_)(C_)(S0),
    "4! C!"     -> CouldMatchSameMerges(F0)(C1)(S0 | S1),
    "CX CX"     -> SamePolarityMerge(C_)(C_)(Nil),
    "CX C!"     -> CouldMatchSameConflicts(C0)(C0)(S0),
    "CX C!"     -> CouldMatchSameMerges(C1)(C0)(S0),
    "C! C!"     -> CouldMatchSameConflicts(C_)(C_)(S0),
    "C! C!"     -> CouldMatchSameMerges(C0)(C1)(S0 | S1),
    "CX !!X"    -> IncomingCouldMatch(R0)(C_)(Nil),
    "(!4) !4"   -> coveredBy("!4 (!4)"),
    "(!4) (!C)" -> coveredBy("(!4) !C"),
    "(!4) (4!)" -> VolatileEvent(S0 | F_)(S0 | F_)(Nil),
    "(!4) (C!)" -> PersistentNoMatch(S0 | F_)(C_ | S0)(Nil),
    "(!4) 4X"   -> VolatileEvent(S0 | F_)(F_)(Nil),
    "(!4) CX"   -> VolatileEvent(S0 | F_)(C_)(Nil),
    "(!C) (!C)" -> PersistentNoMatch(S0 | C_)(S0 | C_)(Nil),
    "(!C) (4!)" -> VolatileEvent(S0 | C_)(F_ | S0)(Nil),
    "(!C) (C!)" -> PersistentNoMatch(S0 | C_)(C_ | S0)(Nil),
    "(!C) 4!"   -> PersistentCouldMatch(S0 | C_)(F_)(S0),
    "(!C) 4X"   -> PersistentNoMatch(S0 | C_)(F_)(Nil),
    "(!C) C!"   -> PersistentCouldMatch(S0 | C_)(C_)(S0),
    "(!C) CX"   -> PersistentNoMatch(S0 | C_)(C_)(Nil),
    "(4!) (4!)" -> VolatileEvent(F_ | S0)(F_ | S0)(Nil),
    "(4!) (C!)" -> VolatileEvent(F_ | S0)(C_ | S0)(Nil),
    "(4!) 4!"   -> VolatileEvent(F1 | S1)(F_)(S0),
    "(4!) C!"   -> VolatileEvent(F1 | S1)(C_)(S0),
    "(C!) C!"   -> PersistentCouldMatch(C_ | S0)(C_)(S0),
    "4! (4!)"   -> VolatileEvent(F_)(F1 | S1)(S0),
    "4! (C!)"   -> PersistentCouldMatch(F_)(C_ | S0)(S0),
    "4X (4!)"   -> VolatileEvent(F_)(F_ | S0)(Nil),
    "4X (C!)"   -> PersistentNoMatch(F_)(C_ | S0)(Nil),
    "C! (C!)"   -> PersistentNoMatch(C_)(C1 | S1)(S0),
    "CX (C!)"   -> PersistentNoMatch(C_)(C_ | S0)(Nil),
    "(!4) 4!"   -> VolatileEvent(S1 | F1)(F_)(S0),
    "(!4) !C"   -> VolatileEvent(S1 | F1)(S0)(C0),
    "(!4) C!"   -> VolatileEvent(S0 | F0)(C_)(S1),
    "(4!) CX"   -> VolatileEvent(F_ | S0)(C_)(Nil),
    "(C!) (C!)" -> PersistentNoMatch(C_ | S0)(C_ | S0)(Nil)
  )

  it should "respect mergeability rules when merging blocks" in effectTest {
    mergeabilityCases
      .map {
        case (key, test) =>
          test.attempt.map(_.fold(_ => s"$key FAILED", _ => key))
      }
      .parSequence
      .map(_.filter(_.contains("FAILED")))
      .map { r =>
        r shouldBe empty
      }
  }

  private[this] def conflicts(b1: Rho, b2: Rho, base: Rho)(
      implicit file: sourcecode.File,
      line: sourcecode.Line
  ) =
    randomDiamondConflictCheck(base, b1, b2, numberOfParentsForDiamondTip = 1)

  private[this] def merges(b1: Rho, b2: Rho, base: Rho)(
      implicit file: sourcecode.File,
      line: sourcecode.Line
  ) =
    randomDiamondConflictCheck(base, b1, b2, numberOfParentsForDiamondTip = 2)

  private[this] def randomDiamondConflictCheck(
      base: Rho,
      b1: Rho,
      b2: Rho,
      numberOfParentsForDiamondTip: Int
  )(implicit file: sourcecode.File, line: sourcecode.Line): Effect[Unit] = {
    val shuffledBlocks = shuffle(Seq(b1, b2))
    diamondConflictCheck(base, shuffledBlocks(0), shuffledBlocks(1), numberOfParentsForDiamondTip)
  }

  private[this] def coveredBy(equivalent: String) = ().pure[Effect]

  private[this] def diamondConflictCheck(
      base: Rho,
      b1: Rho,
      b2: Rho,
      numberOfParentsForDiamondTip: Int
  )(implicit file: sourcecode.File, line: sourcecode.Line): Effect[Unit] =
    Vector(
      ConstructDeploy.sourceDeployNowF[Effect](base.value),
      ConstructDeploy.sourceDeployNowF[Effect](b1.value),
      ConstructDeploy.sourceDeployNowF[Effect](b2.value),
      ConstructDeploy.sourceDeployNowF[Effect]("Nil")
    ).sequence[Effect, DeployData]
      .flatMap { deploys =>
        HashSetCasperTestNode.networkEff(genesis, networkSize = 2).use { nodes =>
          for {
            _ <- nodes(0).addBlock(deploys(0))
            _ <- nodes(1).receive()
            _ <- nodes(0).addBlock(deploys(1))
            _ <- nodes(1).addBlock(deploys(2))
            _ <- nodes(0).receive()

            multiParentBlock <- nodes(0).addBlock(deploys(3))

            _ = nodes(0).logEff.warns.isEmpty shouldBe true
            _ = multiParentBlock.header.get.parentsHashList.size shouldBe numberOfParentsForDiamondTip
            _ = nodes(0).casperEff.contains(multiParentBlock.blockHash) shouldBeF true
          } yield ()
        }
      }
      .adaptError {
        case e: Throwable =>
          new TestFailedException(s"""Expected
               | base = ${base.value}
               | b1   = ${b1.value}
               | b2   = ${b2.value}
               |
               | to produce a merge block with $numberOfParentsForDiamondTip parents, but it didn't
               |
               | go see it at ${file.value}:${line.value}
               | """.stripMargin, e, 5).severedAtStackDepth
      }

  it should "not produce UnusedCommEvent while merging non conflicting blocks in the presence of conflicting ones" in effectTest {

    val registryRho =
      """
        |// Expected output
        |//
        |// "REGISTRY_SIMPLE_INSERT_TEST: create arbitrary process X to store in the registry"
        |// Unforgeable(0xd3f4cbdcc634e7d6f8edb05689395fef7e190f68fe3a2712e2a9bbe21eb6dd10)
        |// "REGISTRY_SIMPLE_INSERT_TEST: adding X to the registry and getting back a new identifier"
        |// `rho:id:pnrunpy1yntnsi63hm9pmbg8m1h1h9spyn7zrbh1mcf6pcsdunxcci`
        |// "REGISTRY_SIMPLE_INSERT_TEST: got an identifier for X from the registry"
        |// "REGISTRY_SIMPLE_LOOKUP_TEST: looking up X in the registry using identifier"
        |// "REGISTRY_SIMPLE_LOOKUP_TEST: got X from the registry using identifier"
        |// Unforgeable(0xd3f4cbdcc634e7d6f8edb05689395fef7e190f68fe3a2712e2a9bbe21eb6dd10)
        |
        |new simpleInsertTest, simpleInsertTestReturnID, simpleLookupTest,
        |    signedInsertTest, signedInsertTestReturnID, signedLookupTest,
        |    ri(`rho:registry:insertArbitrary`),
        |    rl(`rho:registry:lookup`),
        |    stdout(`rho:io:stdout`),
        |    stdoutAck(`rho:io:stdoutAck`), ack in {
        |        simpleInsertTest!(*simpleInsertTestReturnID) |
        |        for(@idFromTest1 <- simpleInsertTestReturnID) {
        |            simpleLookupTest!(idFromTest1, *ack)
        |        } |
        |
        |        contract simpleInsertTest(registryIdentifier) = {
        |            stdout!("REGISTRY_SIMPLE_INSERT_TEST: create arbitrary process X to store in the registry") |
        |            new X, Y, innerAck in {
        |                stdoutAck!(*X, *innerAck) |
        |                for(_ <- innerAck){
        |                    stdout!("REGISTRY_SIMPLE_INSERT_TEST: adding X to the registry and getting back a new identifier") |
        |                    ri!(*X, *Y) |
        |                    for(@uri <- Y) {
        |                        stdout!("REGISTRY_SIMPLE_INSERT_TEST: got an identifier for X from the registry") |
        |                        stdout!(uri) |
        |                        registryIdentifier!(uri)
        |                    }
        |                }
        |            }
        |        } |
        |
        |        contract simpleLookupTest(@uri, result) = {
        |            stdout!("REGISTRY_SIMPLE_LOOKUP_TEST: looking up X in the registry using identifier") |
        |            new lookupResponse in {
        |                rl!(uri, *lookupResponse) |
        |                for(@val <- lookupResponse) {
        |                    stdout!("REGISTRY_SIMPLE_LOOKUP_TEST: got X from the registry using identifier") |
        |                    stdoutAck!(val, *result)
        |                }
        |            }
        |        }
        |    }
      """.stripMargin

    val tuplesRho =
      """
        |// tuples only support random access
        |new stdout(`rho:io:stdout`) in {
        |
        |  // prints 2 because tuples are 0-indexed
        |  stdout!((1,2,3).nth(1))
        |}
      """.stripMargin
    val timeRho =
      """
        |new getBlockData(`rho:block:data`), stdout(`rho:io:stdout`), tCh in {
        |  getBlockData!(*tCh) |
        |  for(@_, @t <- tCh) {
        |    match t {
        |      Nil => { stdout!("no block time; no blocks yet? Not connected to Casper network?") }
        |      _ => { stdout!({"block time": t}) }
        |    }
        |  }
        |}
      """.stripMargin

    HashSetCasperTestNode.networkEff(genesis, networkSize = 3).use { nodes =>
      val n1     = nodes(0)
      val n2     = nodes(1)
      val n3     = nodes(2)
      val short  = ConstructDeploy.sourceDeploy("new x in { x!(0) }", 1L)
      val time   = ConstructDeploy.sourceDeploy(timeRho, 3L)
      val tuples = ConstructDeploy.sourceDeploy(tuplesRho, 2L)
      val reg    = ConstructDeploy.sourceDeploy(registryRho, 4L)
      for {
        b1n3 <- n3.addBlock(short)
        b1n2 <- n2.addBlock(time)
        b1n1 <- n1.addBlock(tuples)
        _    <- n2.receive()
        b2n2 <- n2.createBlock(reg)
      } yield ()
    }
  }

  it should "not merge blocks that touch the same channel involving joins" in effectTest {
    HashSetCasperTestNode.networkEff(genesis, networkSize = 2).use { nodes =>
      for {
        current0 <- timeEff.currentMillis
        deploy0 = ConstructDeploy.sourceDeploy(
          "@1!(47)",
          current0
        )
        current1 <- timeEff.currentMillis
        deploy1 = ConstructDeploy.sourceDeploy(
          "for(@x <- @1; @y <- @2){ @1!(x) }",
          current1
        )
        deploy2 <- ConstructDeploy.basicDeployData[Effect](2)
        deploys = Vector(
          deploy0,
          deploy1,
          deploy2
        )

        block0 <- nodes(0).addBlock(deploys(0))
        block1 <- nodes(1).addBlock(deploys(1))
        _      <- nodes(0).receive()
        _      <- nodes(1).receive()
        _      <- nodes(0).receive()
        _      <- nodes(1).receive()

        singleParentBlock <- nodes(0).addBlock(deploys(2))
        _                 <- nodes(1).receive()

        _      = nodes(0).logEff.warns.isEmpty shouldBe true
        _      = nodes(1).logEff.warns.isEmpty shouldBe true
        _      = singleParentBlock.header.get.parentsHashList.size shouldBe 1
        _      <- nodes(0).casperEff.contains(singleParentBlock.blockHash) shouldBeF true
        result <- nodes(1).casperEff.contains(singleParentBlock.blockHash) shouldBeF true
      } yield result
    }
  }

  "This spec" should "cover all mergeability cases" in {
    val allMergeabilityCases = {
      val events = List(
        "!X",
        "!4",
        "!C",
        "4X",
        "4!",
        "4!!",
        "PX",
        "P!",
        "P!!",
        "!!X",
        "!!4",
        "!!C",
        "CX",
        "C!",
        "C!!"
      )

      val pairs    = events.combinations(2)
      val diagonal = events.map(x => List(x, x))
      val cases    = (pairs ++ diagonal).toList

      def isComm(s: String) = !s.contains("X")

      def makeVolatile(s: String): List[String] = isComm(s) match {
        case false => List(s)
        case true  => List(s, s"($s)")
      }

      def makeVolatiles(v: List[String]): List[List[String]] =
        for {
          a <- makeVolatile(v(0))
          b <- makeVolatile(v(1))
        } yield List(a, b)

      val withVolatiles = cases.flatMap(makeVolatiles)

      // TODO: Do not filter out missing cases
      withVolatiles
        .map(_.mkString(" "))
        .filterNot(_.contains("P"))
        .filterNot(_.contains("!!"))
        .toSet
    }

    val testedMergeabilityCases = mergeabilityCases.map(_._1)
    withClue(s"""Missing cases: ${allMergeabilityCases
      .diff(testedMergeabilityCases.toSet)
      .toList
      .sorted
      .mkString(", ")}\n""") {
      testedMergeabilityCases should contain allElementsOf allMergeabilityCases
    }
  }

}
