package suwayomi.tachidesk.opds.dto

data class OpdsSearchCriteria(
    val query: String? = null,
    val author: String? = null,
    val artist: String? = null,
    val title: String? = null,
)
