"""MDX generator module for creating connector documentation."""

from ..constants import CONTENT_START_MARKER, CONTENT_END_MARKER, AUTH_TYPE_DESCRIPTIONS


def escape_mdx_special_chars(text):
    """Escape special characters that could cause issues in MDX parsing.

    Args:
        text (str): The text to escape

    Returns:
        str: Text with special characters escaped
    """
    if not text:
        return text

    # Replace angle brackets with their HTML entity equivalents
    escaped_text = text.replace("<", "&lt;").replace(">", "&gt;")

    # Debug statement to verify the function is working
    print(f"Escaping text: '{text}' -> '{escaped_text}'")

    return escaped_text


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

                        # Escape special characters in description
                        escaped_description = escape_mdx_special_chars(field_description)

                        # Generate ParamField component instead of table row
                        # Use proper JSX syntax with curly braces for boolean values
                        content += f"""<ParamField
  name="{field['name']}"
  type="{field['type']}"
  required={{{'true' if field['required'] else 'false'}}}
>
  {escaped_description}
</ParamField>
"""
                    content += "\n"
            elif (
                auth_type == "oauth2"
                or auth_type == "oauth2_with_refresh"
                or auth_type == "oauth2_with_refresh_rotating"
            ):
                content += "You can connect through the Airweave UI, which will guide you through the OAuth flow.\n\n"
            elif auth_type == "none":
                content += "This connector does not require authentication.\n\n"
            else:
                content += (
                    "Please refer to the Airweave documentation for authentication details.\n\n"
                )

    # Add GitHub reference card between Configuration and Entities sections
    content += f"""
<Card
  title="View Source Code"
  icon="brands github"
  href="https://github.com/airweave-ai/airweave/tree/main/backend/airweave/platform/sources/{connector_name}.py"
>
  Explore the {display_name} connector implementation
</Card>
"""

    # Add entity information
    if entity_info:
        content += "\n## Entities\n\n"
        content += "The following data models are available for this connector:\n\n"

        for entity in entity_info:
            # Start with opening Accordion tag
            content += f"<Accordion title=\"{entity['name']}\">\n\n"
            content += f"{entity['docstring']}\n\n"

            # Use markdown tables for entity fields
            content += "| Field | Type | Description |\n"
            content += "|-------|------|-------------|\n"
            for field in entity["fields"]:
                # Escape special characters in the description
                escaped_description = escape_mdx_special_chars(field["description"])
                content += f"| {field['name']} | {field['type']} | {escaped_description} |\n"

            content += "\n</Accordion>\n"

    # Wrap the content with delimiters
    return f"{CONTENT_START_MARKER}\n\n{content}\n\n{CONTENT_END_MARKER}"
