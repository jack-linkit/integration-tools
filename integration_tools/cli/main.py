"""
Main CLI entry point for integration tools.
"""

import click
from integration_tools.cli.enhanced_request_replayer import cli as request_cli


@click.group()
@click.version_option()
@click.pass_context
def main(ctx):
    """Integration Tools - Enhanced request management and integration utilities."""
    # Initialize the context object properly for imported commands
    ctx.ensure_object(dict)
    ctx.obj['logger'] = None  # Will be set by individual commands
    ctx.obj['request_manager'] = None
    ctx.obj['workflows'] = None


# Import all commands from enhanced_request_replayer
for name, command in request_cli.commands.items():
    main.add_command(command, name=name)


if __name__ == "__main__":
    main()