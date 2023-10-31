<table>
    <tr>
        <th>Name</th>
        <th>Mandatory</th>
        <th>Description</th>
        <th>Example</th>
        <th>Default</th>
    </tr>
    {% for field in include.fields %}
    <tr>
        <td>{{ field.name }}</td>
        <td>{{ field.mandatory |  default: "No" }}</td>
        <td>{{ field.description }}</td>
        <td>{{ field.example }}</td>
        <td>{{ field.default }}</td>
    </tr>
    {% endfor %}
</table>
