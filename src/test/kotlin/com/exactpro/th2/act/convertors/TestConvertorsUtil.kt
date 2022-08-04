package com.exactpro.th2.act.convertors

import com.exactpro.th2.act.grpc.NoPartyIDs
import com.exactpro.th2.act.grpc.Quote
import com.exactpro.th2.act.grpc.TradingParty
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.value.toValue

fun tradingParty(quantity: Int): TradingParty {
    val tradingParty = TradingParty.newBuilder()
    for (partyRole in 0..quantity) {
        tradingParty.addNoPartyIds(
            NoPartyIDs.newBuilder()
                .setPartyId("PartyID")
                .setPartyIdSource("PartyIDSource")
                .setPartyRole(partyRole).build()
        )
    }
    return tradingParty.build()
}

fun noPartyIDsList(quantity: Int): List<Message> {
    val noPartyIDs = mutableListOf<Message>()
    for (partyRole in 0..quantity) {
        noPartyIDs.add(
            Message.newBuilder()
                .putFields("PartyID", "PartyID".toValue())
                .putFields("PartyIDSource", "PartyIDSource".toValue())
                .putFields("PartyRole", partyRole.toValue()).build()
        )
    }
    return noPartyIDs
}

fun quoteQualifierList(quantity: Int): List<Message> {
    val quoteQualifier = mutableListOf<Message>()
    for (partyRole in 0..quantity) {
        quoteQualifier.add(
            Message.newBuilder()
                .putFields("QuoteQualifier", "QuoteQualifier".toValue()).build()
        )
    }
    return quoteQualifier
}