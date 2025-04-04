package com.amadeus.dataio.config

package object fields
    extends DateFilterConfigurator
    with DropDuplicatesConfigurator
    with OptionsConfigurator
    with RepartitionConfigurator
    with SortWithinPartitionsConfigurator
    with CoalesceConfigurator
    with RepartitionByRangeConfigurator
    with PathConfigurator
    with PartitionByConfigurator
    with TimeoutConfigurator
    with SchemaConfigurator
    with StreamingTriggerConfigurator
