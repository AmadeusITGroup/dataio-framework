package com.amadeus.dataio.pipelines

/**
 * Test purpose case class
 *
 * @param date the trip date
 * @param from the trip departure
 * @param to the trip arrival
 * @param via the trip via point
 */
protected case class TestTrip(date: String, from: String, to: String, via: String)
