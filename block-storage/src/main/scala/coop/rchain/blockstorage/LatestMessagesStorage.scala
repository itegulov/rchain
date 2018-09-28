package coop.rchain.blockstorage

import com.google.protobuf.ByteString
import LatestMessagesStorage.{BlockHash, Validator}

trait LatestMessagesStorage[F[_]] {
  def access[A](fa: F[Map[Validator, BlockHash] => A]): F[A]

  def insert(validator: Validator, blockHash: BlockHash): F[Unit]
}

object LatestMessagesStorage {
  type Validator = ByteString
  type BlockHash = ByteString
}
