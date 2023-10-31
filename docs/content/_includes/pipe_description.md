{{ include.pipe.common.description }}

{% if include.pipe.common.top_warning %}
{{ include.pipe.common.top_warning }}
{: .warning}
{% endif %}

{% assign root_package = "https://github.com/AmadeusITGroup/dataio-framework/tree/main/src/main/scala/com/amadeus/dataio" %}
{% assign code_directory =  root_package | append: "/pipes/" | append: include.pipe.common.code_directory %}

{% if include.pipe.common.code_directory %}
Code repository: <a href="{{  code_directory  }}" target="_blank">{{  code_directory  }}</a>
{% endif %}

{% if include.pipe.common.links %}
Useful links:
<ul>
    {% for link in include.pipe.common.links %}
        <li><a href="{{ link.url }}" target="_blank">{{ link.name }}</a></li>
    {% endfor %}
</ul>
{% endif %}

{% if include.pipe.common.fields %}
---

## Common
The following fields are available for all {{ include.pipe.common.name }} components:

{% include fields_table.md fields=include.pipe.common.fields %}

{% if include.pipe.common.fields_warning %}
{{ include.pipe.common.fields_warning }}
{: .warning}
{% endif %}

{% endif %}

{% if include.pipe.batch.input or include.pipe.batch.output %}
---

## Batch
{% if include.pipe.batch.input %}
### Input
**Type:** {{ include.pipe.batch.input.type }}
{% if include.pipe.batch.input.fields %}
{% include fields_table.md fields=include.pipe.batch.input.fields %}
{% endif %}
{% else %}
No batch input is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}

{% if include.pipe.batch.output %}
### Output
**Type:** {{ include.pipe.batch.output.type }}
{% if include.pipe.batch.output.fields %}
{% include fields_table.md fields=include.pipe.batch.output.fields %}
{% endif %}
{% else %}
No batch output is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}

{% else %}
No batch processing is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}

{% if include.pipe.streaming.input or include.pipe.streaming.output %}
--- 

## Streaming
{% if include.pipe.streaming.input %}
### Input
**Type:** {{ include.pipe.streaming.input.type }}
{% if include.pipe.streaming.input.fields %}
{% include fields_table.md fields=include.pipe.streaming.input.fields %}
{% endif %}
{% else %}
No streaming input is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}

{% if include.pipe.streaming.output %}
### Output
**Type:** {{ include.pipe.streaming.output.type }}
{% if include.pipe.streaming.output.fields %}
{% include fields_table.md fields=include.pipe.streaming.output.fields %}
{% endif %}
{% else %}
No streaming output is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}

{% else %}
No streaming processing is currently available for {{  include.pipe.common.name  }} in Data I/O.
{: .warning}
{% endif %}
