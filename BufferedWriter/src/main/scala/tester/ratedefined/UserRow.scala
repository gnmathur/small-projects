package tester.ratedefined

import java.time.LocalDateTime

// Data model for rows to be inserted; The producer will generate this data and put on the queue. The consumers
// will dequeue and turn this information into SQL insert statements
case class UserRow(id: Int, name: String, balance: Double, createdDate: LocalDateTime)