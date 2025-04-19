"""MDX generator module for creating connector documentation."""

from ..constants import CONTENT_START_MARKER, CONTENT_END_MARKER, AUTH_TYPE_DESCRIPTIONS


def generate_mdx_content(connector_name, entity_info, source_info, auth_configs):
    """Generate MDX content for a connector.

    Args:
        connector_name (str): The name of the connector
        entity_info (list): List of entity class information
        source_info (list): List of source class information
        auth_configs (dict): Dictionary of auth config classes

    Returns:
        str: The generated MDX content
    """
    # Normalize connector name for display
    display_name = connector_name.replace("_", " ").title()

    content = f"""<div className="connector-header">
  <img src="icon.svg" alt="{display_name} logo" width="72" height="72" className="connector-icon" />
  <div className="connector-info">
    <h1>{display_name}</h1>
    <p>Connect your {display_name} data to Airweave</p>
  </div>
</div>

## Overview

The {display_name} connector allows you to sync data from {display_name} into Airweave, making it available for search and retrieval by your agents.
"""

    # Add source information
    if source_info:
        content += "\n## Configuration\n\n"
        for source in source_info:
            content += f"""
### {source['name']}

{source['docstring']}

"""
            # Add authentication information section
            content += "#### Authentication\n\n"

            auth_type = source.get("auth_type")
            auth_config_class = source.get("auth_config_class")

            if auth_type:
                auth_type_display = AUTH_TYPE_DESCRIPTIONS.get(auth_type, auth_type)
                content += f"This connector uses **{auth_type_display}**.\n\n"

            # If auth_config_class is available and matches an entry in auth_configs, display its fields
            if auth_config_class and auth_config_class in auth_configs:
                auth_info = auth_configs[auth_config_class]
                content += f"Authentication configuration class: `{auth_config_class}`\n\n"

                if auth_info["docstring"]:
                    content += f"{auth_info['docstring']}\n\n"

                if auth_info["fields"]:
                    content += "The following configuration fields are required:\n\n"
                    content += "| Field | Type | Description | Required |\n"
                    content += "|-------|------|-------------|----------|\n"
                    for field in auth_info["fields"]:
                        # Get descriptions from parent class if available
                        field_description = field["description"]
                        if field_description == "No description" and "parent_class" in auth_info:
                            parent_class = auth_info["parent_class"]
                            if parent_class in auth_configs:
                                parent_fields = auth_configs[parent_class]["fields"]
                                for parent_field in parent_fields:
                                    if (
                                        parent_field["name"] == field["name"]
                                        and parent_field["description"] != "No description"
                                    ):
                                        field_description = parent_field["description"]
                                        break

                        content += f"| {field['name']} | {field['type']} | {field_description} | {'Yes' if field['required'] else 'No'} |\n"
                    content += "\n"
            elif (
                auth_type == "oauth2"
                or auth_type == "oauth2_with_refresh"
                or auth_type == "oauth2_with_refresh_rotating"
            ):
                content += "This connector uses OAuth authentication. You can connect through the Airweave UI, which will guide you through the OAuth flow.\n\n"
            elif auth_type == "none":
                content += "This connector does not require authentication.\n\n"
            else:
                content += (
                    "Please refer to the Airweave documentation for authentication details.\n\n"
                )

    # Add entity information
    if entity_info:
        content += "\n## Entities\n\n"
        content += "The following data models are available for this connector:\n\n"

        for entity in entity_info:
            content += f"""
<details>
<summary><strong>{entity['name']}</strong></summary>

{entity['docstring']}

| Field | Type | Description |
|-------|------|-------------|
"""
            for field in entity["fields"]:
                content += f"| {field['name']} | {field['type']} | {field['description']} |\n"

            content += "\n</details>\n"

    # Wrap the content with delimiters
    return f"{CONTENT_START_MARKER}\n\n{content}\n\n{CONTENT_END_MARKER}"
