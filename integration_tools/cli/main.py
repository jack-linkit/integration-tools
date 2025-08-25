"""
Main CLI entry point for integration tools.
"""

import click
from integration_tools.cli.enhanced_request_replayer import cli as request_cli


@click.group()
@click.version_option()
def main():
    """Integration Tools - Enhanced request management and integration utilities."""
    pass


# Import all commands from enhanced_request_replayer
for name, command in request_cli.commands.items():
    main.add_command(command, name=name)


if __name__ == "__main__":
    main()