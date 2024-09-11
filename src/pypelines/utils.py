import re


def to_snake_case(text):
    # Replace spaces or hyphens with underscores
    text = re.sub(r"[\s-]+", "_", text)
    # Convert CamelCase to snake_case
    text = re.sub(r"([a-z])([A-Z])", r"\1_\2", text)
    # Convert all characters to lowercase
    return text.lower()
