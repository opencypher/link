package org.opencypher.link.testing.fixture

trait TestDataFixture {

  def dataFixture: String

  def nbrNodes: Int
  def nbrRels: Int
}