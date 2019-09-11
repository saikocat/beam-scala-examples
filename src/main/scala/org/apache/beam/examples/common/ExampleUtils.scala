package org.apache.beam.examples.common

object ExampleUtils {

  final val SC_NOT_FOUND = 404

  /**
    * \p{L} denotes the category of Unicode letters, so this pattern will match on everything that is
    * not a letter.
    *
    * It is used for tokenizing strings in the wordcount examples.
    */
  final val TOKENIZER_PATTERN = "[^\\p{L}]+"

}
