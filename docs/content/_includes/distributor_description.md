{{ include.distributor.description }}

{% if include.distributor.top_warning %}
{{ include.distributor.top_warning }}
{: .warning}
{% endif %}


{% if include.distributor.code_directory %}
{% assign root_package = "https://github.com/AmadeusITGroup/dataio-framework/tree/main/src/main/scala/com/amadeus/dataio" %}
{% assign code_directory =  root_package | append: "/distributors/" | append: include.distributor.code_directory %}

Code repository: <a href="{{  code_directory  }}" target="_blank">{{  code_directory  }}</a>
{% endif %}

{% if include.distributor.links %}
Useful links:
<ul>
    {% for link in include.distributor.links %}
        <li><a href="{{ link.url }}" target="_blank">{{ link.name }}</a></li>
    {% endfor %}
</ul>
{% endif %}

{% if include.distributor.fields %}
---

## Fields
The following fields are available for {{ include.distributor.name }} distributor components:

{% include fields_table.md fields=include.distributor.fields %}

{% if include.distributor.fields_warning %}
{{ include.distributor.fields_warning }}
{: .warning}
{% endif %}

{% endif %}
