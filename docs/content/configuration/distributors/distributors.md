---
title: Distributors
layout: default
parent: Configuration
has_children: true
nav_order: 11
fields:
- name: Name
  description: The name of the distributor, that can be used to access it from the HandlerAccessor.
  example: Name = "email-sender"
- name: Type
  mandatory: "Yes"
  description: The fully qualified name of the class to use as distributor.
  example: Type = "com.amadeus.dataio.distributors.email.EmailDistributor"
---
# Distributors

Data I/O introduces a feature known as Distributors. Distributors play a crucial role in the framework by facilitating the efficient transfer of files to their designated destinations, such as email addresses, remote servers, etc.

Distributors are configured within the Distribution section of your configuration. In this documentation, we will explore how to configure and utilize Distributors for seamless file distribution.

## Common Fields

Similarily to pipes, distributors have access to at least two fields: Name and Type.

{% include fields_table.md fields=page.fields %}

Although the name is not mandatory, it is highly recommended to specify it unless you are using a specific type of processor that automatically fetches distributors by indexes, rather than names (although no such processor type currently exists by default).
{: .warning}

