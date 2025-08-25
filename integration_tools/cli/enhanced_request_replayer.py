"""
Enhanced Request Replayer CLI with rich interface and batch operations.
"""

import asyncio
import json
import os
from typing import List, Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich import print as rprint

from integration_tools.core.async_request_manager import AsyncRequestManager
from integration_tools.workflows.common_workflows import CommonWorkflows
from integration_tools.core.error_handling import setup_logging, ErrorHandler


console = Console()
error_handler = ErrorHandler()


@click.group()
@click.option('--log-level', default='INFO', help='Logging level')
@click.option('--log-file', help='Optional log file path')
@click.pass_context
def cli(ctx, log_level, log_file):
    """Enhanced Request Replayer with batch operations and workflows."""
    ctx.ensure_object(dict)
    ctx.obj['logger'] = setup_logging(log_level, log_file)
    ctx.obj['request_manager'] = AsyncRequestManager()
    ctx.obj['workflows'] = CommonWorkflows(ctx.obj['request_manager'])


@cli.command()
@click.option('--filter', 'name_filter', help='Filter by name contains')
@click.pass_context
def list_types(ctx, name_filter):
    """List DataRequestTypes with optional filtering."""
    try:
        rm = ctx.obj['request_manager']
        types = rm.list_request_types(name_filter)
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", style="dim", width=6)
        table.add_column("Name")
        
        for type_id, name in types:
            table.add_row(str(type_id), name or "")
        
        console.print(table)
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'listing request types')}")


@cli.command()
@click.option('--type-ids', help='Comma-separated DataRequestTypeIDs')
@click.option('--type-names', help='Comma-separated type name prefixes')
@click.option('--district-ids', help='Comma-separated DistrictIDs')
@click.option('--json-output', is_flag=True, help='Output as JSON')
@click.option('--save-csv', help='Save results to CSV file')
@click.pass_context
def find_requests(ctx, type_ids, type_names, district_ids, json_output, save_csv):
    """Find latest requests by various criteria."""
    try:
        rm = ctx.obj['request_manager']
        
        # Parse input parameters
        type_id_list = [int(x.strip()) for x in type_ids.split(',')] if type_ids else None
        type_name_list = [x.strip() for x in type_names.split(',')] if type_names else None
        district_id_list = [int(x.strip()) for x in district_ids.split(',')] if district_ids else None
        
        with console.status("[bold green]Searching for requests..."):
            requests = rm.find_requests(type_id_list, type_name_list, district_id_list)
        
        if not requests:
            rprint("[yellow]No matching requests found.[/yellow]")
            return
        
        if json_output:
            output = [
                {
                    "RequestID": r.RequestID,
                    "DistrictID": r.DistrictID,
                    "DataRequestTypeName": r.DataRequestTypeName,
                    "DataRequestTypeID": r.DataRequestTypeID,
                    "ImportedFileName": r.ImportedFileName,
                    "Status": r.Status,
                    "RequestTime": r.RequestTime.isoformat() if r.RequestTime else None
                }
                for r in requests
            ]
            console.print_json(json.dumps(output, indent=2))
        else:
            table = Table(show_header=True, header_style="bold blue")
            table.add_column("RequestID", width=10)
            table.add_column("DistrictID", width=10)
            table.add_column("Type Name", width=20)
            table.add_column("Type ID", width=8)
            table.add_column("Status", width=8)
            table.add_column("Request Time", width=18)
            
            for r in requests:
                status_color = "green" if r.Status == 5 else "red" if r.Status == 4 else "yellow"
                table.add_row(
                    str(r.RequestID),
                    str(r.DistrictID),
                    r.DataRequestTypeName or "",
                    str(r.DataRequestTypeID),
                    f"[{status_color}]{r.Status}[/{status_color}]",
                    r.RequestTime.strftime("%Y-%m-%d %H:%M:%S") if r.RequestTime else ""
                )
            
            console.print(table)
            rprint(f"\n[bold]Found {len(requests)} requests[/bold]")
        
        if save_csv:
            import csv
            with open(save_csv, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['RequestID', 'DistrictID', 'TypeName', 'TypeID', 'Status', 'RequestTime'])
                for r in requests:
                    writer.writerow([
                        r.RequestID, r.DistrictID, r.DataRequestTypeName, 
                        r.DataRequestTypeID, r.Status,
                        r.RequestTime.strftime("%Y-%m-%d %H:%M:%S") if r.RequestTime else ""
                    ])
            rprint(f"[green]Results saved to {save_csv}[/green]")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'finding requests')}")


@cli.command()
@click.argument('request_id', type=int)
@click.pass_context
def show_email(ctx, request_id):
    """Show email content for a request in browser."""
    try:
        rm = ctx.obj['request_manager']
        
        with console.status(f"[bold green]Fetching email content for RequestID {request_id}..."):
            success = rm.show_email_content(request_id)
        
        if success:
            rprint(f"[green]Successfully opened email content for RequestID {request_id}[/green]")
        else:
            rprint(f"[yellow]No email content found for RequestID {request_id}[/yellow]")
            
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'showing email content')}")


@cli.command()
@click.argument('request_ids')
@click.option('--local-dir', default='~/Downloads', help='Local download directory')
@click.option('--max-concurrent', default=5, help='Maximum concurrent downloads')
@click.pass_context
def download(ctx, request_ids, local_dir, max_concurrent):
    """Download files for requests (comma-separated IDs)."""
    try:
        rm = ctx.obj['request_manager']
        request_id_list = [int(x.strip()) for x in request_ids.split(',')]
        
        rprint(f"[bold]Starting download of {len(request_id_list)} requests...[/bold]")
        
        async def run_download():
            return await rm.download_files_batch(
                request_id_list, 
                local_dir=local_dir, 
                max_concurrent=max_concurrent,
                show_progress=True
            )
        
        results = asyncio.run(run_download())
        
        # Display results
        successful = sum(1 for r in results.values() if r.get("success"))
        total_files = sum(r.get("files_downloaded", 0) for r in results.values())
        
        rprint(f"\n[bold green]Download complete:[/bold green]")
        rprint(f"  • Successful requests: {successful}/{len(request_id_list)}")
        rprint(f"  • Total files downloaded: {total_files}")
        
        if successful < len(request_id_list):
            rprint("\n[bold yellow]Failed downloads:[/bold yellow]")
            for req_id, result in results.items():
                if not result.get("success"):
                    rprint(f"  • RequestID {req_id}: {result.get('message', 'Unknown error')}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'downloading files')}")


@cli.command()
@click.argument('request_ids')
@click.option('--temp-dir', default='/tmp', help='Temporary directory for processing')
@click.option('--max-concurrent', default=3, help='Maximum concurrent restores')
@click.pass_context
def restore(ctx, request_ids, temp_dir, max_concurrent):
    """Restore processed files for requests back to SFTP."""
    try:
        rm = ctx.obj['request_manager']
        request_id_list = [int(x.strip()) for x in request_ids.split(',')]
        
        rprint(f"[bold]Starting restore of {len(request_id_list)} requests...[/bold]")
        
        async def run_restore():
            return await rm.restore_files_batch(
                request_id_list,
                temp_dir=temp_dir,
                max_concurrent=max_concurrent,
                show_progress=True
            )
        
        results = asyncio.run(run_restore())
        
        # Display results
        successful = sum(1 for r in results.values() if r.get("success"))
        total_files = sum(r.get("files_moved", 0) for r in results.values())
        
        rprint(f"\n[bold green]Restore complete:[/bold green]")
        rprint(f"  • Successful requests: {successful}/{len(request_id_list)}")
        rprint(f"  • Total files restored: {total_files}")
        
        if successful < len(request_id_list):
            rprint("\n[bold yellow]Failed restores:[/bold yellow]")
            for req_id, result in results.items():
                if not result.get("success"):
                    rprint(f"  • RequestID {req_id}: {result.get('message', 'Unknown error')}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'restoring files')}")


@cli.command()
@click.argument('request_ids')
@click.option('--delete-checksums', is_flag=True, help='Delete checksums before rerun')
@click.option('--checksum-keys', help='Specific checksum keys to delete (comma-separated)')
@click.pass_context
def rerun(ctx, request_ids, delete_checksums, checksum_keys):
    """Re-trigger runs for requests with optional checksum deletion."""
    try:
        rm = ctx.obj['request_manager']
        request_id_list = [int(x.strip()) for x in request_ids.split(',')]
        checksum_key_list = [x.strip() for x in checksum_keys.split(',')] if checksum_keys else None
        
        with console.status(f"[bold green]Processing rerun for {len(request_id_list)} requests..."):
            result = rm.rerun_requests(request_id_list, delete_checksums, checksum_key_list)
        
        rprint(f"[bold green]Rerun operation complete:[/bold green]")
        rprint(f"  • Requests processed: {len(result['request_ids'])}")
        rprint(f"  • Queues updated: {result['queues_updated']}")
        if delete_checksums:
            rprint(f"  • Checksums deleted: {result['checksums_deleted']}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'rerunning requests')}")


# Workflow commands
@cli.group()
def workflow():
    """Run predefined workflows for common operations."""
    pass


@workflow.command()
@click.argument('district_ids')
@click.option('--type-names', default='SAT,PSAT', help='Request types to include')
@click.option('--delete-checksums/--keep-checksums', default=True, help='Delete checksums')
@click.option('--restore-files/--no-restore', default=True, help='Restore files first')
@click.pass_context
def district_refresh(ctx, district_ids, type_names, delete_checksums, restore_files):
    """Complete district refresh workflow."""
    try:
        workflows = ctx.obj['workflows']
        district_id_list = [int(x.strip()) for x in district_ids.split(',')]
        type_name_list = [x.strip() for x in type_names.split(',')]
        
        rprint(f"[bold]Starting district refresh workflow for {len(district_id_list)} districts...[/bold]")
        
        async def run_workflow():
            return await workflows.district_refresh_workflow(
                district_id_list,
                type_name_list,
                delete_checksums=delete_checksums,
                restore_files=restore_files,
                show_progress=True
            )
        
        result = asyncio.run(run_workflow())
        
        if result.success:
            rprint(f"[bold green]✓ Workflow completed successfully[/bold green]")
            rprint(f"  • Steps completed: {result.steps_completed}/{result.total_steps}")
            rprint(f"  • Message: {result.message}")
            
            # Show summary data
            data = result.data
            if 'requests_found' in data:
                rprint(f"  • Requests processed: {data['requests_found']}")
            
            if 'rerun_result' in data:
                rerun = data['rerun_result']
                rprint(f"  • Queues updated: {rerun.get('queues_updated', 0)}")
                if delete_checksums:
                    rprint(f"  • Checksums deleted: {rerun.get('checksums_deleted', 0)}")
        else:
            rprint(f"[bold red]✗ Workflow failed[/bold red]")
            rprint(f"  • Steps completed: {result.steps_completed}/{result.total_steps}")
            rprint(f"  • Error: {result.message}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'district refresh workflow')}")


@workflow.command()
@click.argument('type_names')
@click.option('--district-ids', help='Optional district filter (comma-separated)')
@click.option('--local-dir', default='~/Downloads/bulk_download', help='Local download directory')
@click.option('--max-concurrent', default=5, help='Maximum concurrent downloads')
@click.pass_context
def bulk_download(ctx, type_names, district_ids, local_dir, max_concurrent):
    """Bulk download files for request types."""
    try:
        workflows = ctx.obj['workflows']
        type_name_list = [x.strip() for x in type_names.split(',')]
        district_id_list = [int(x.strip()) for x in district_ids.split(',')] if district_ids else None
        
        rprint(f"[bold]Starting bulk download for {', '.join(type_name_list)}...[/bold]")
        
        async def run_workflow():
            return await workflows.bulk_file_download_workflow(
                type_name_list,
                district_ids=district_id_list,
                local_dir=local_dir,
                max_concurrent=max_concurrent,
                show_progress=True
            )
        
        result = asyncio.run(run_workflow())
        
        if result.success:
            rprint(f"[bold green]✓ Bulk download completed[/bold green]")
            rprint(f"  • {result.message}")
            
            summary = result.data.get('summary', {})
            rprint(f"  • Successful requests: {summary.get('successful_requests', 0)}")
            rprint(f"  • Total files: {summary.get('total_files_downloaded', 0)}")
        else:
            rprint(f"[bold red]✗ Bulk download failed[/bold red]")
            rprint(f"  • Error: {result.message}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'bulk download workflow')}")


@workflow.command()
@click.option('--integration-types', default='SAT,PSAT', help='Integration types to monitor')
@click.option('--days-back', default=7, help='Number of days to look back')
@click.option('--json-output', is_flag=True, help='Output detailed JSON report')
@click.pass_context
def monitor(ctx, integration_types, days_back, json_output):
    """Monitor integration health and performance."""
    try:
        workflows = ctx.obj['workflows']
        integration_type_list = [x.strip() for x in integration_types.split(',')]
        
        rprint(f"[bold]Monitoring {', '.join(integration_type_list)} integrations...[/bold]")
        
        async def run_monitoring():
            return await workflows.integration_monitoring_workflow(
                integration_type_list,
                days_back=days_back,
                show_progress=True
            )
        
        result = asyncio.run(run_monitoring())
        
        if result.success:
            data = result.data
            
            rprint(f"\n[bold green]Integration Health Report[/bold green]")
            rprint(f"  • Success Rate: {data['success_rate']}%")
            rprint(f"  • Total Requests: {data['total_requests']}")
            
            summary = data.get('summary', {})
            rprint(f"  • Healthy Districts: {summary.get('healthy_districts', 0)}")
            rprint(f"  • Districts with Issues: {summary.get('districts_with_issues', 0)}")
            
            if summary.get('most_problematic_type'):
                rprint(f"  • Most Problematic Type: {summary['most_problematic_type']}")
            
            if json_output:
                console.print_json(json.dumps(data, indent=2))
            
        else:
            rprint(f"[bold red]✗ Monitoring failed[/bold red]")
            rprint(f"  • Error: {result.message}")
        
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'monitoring workflow')}")


@cli.command()
@click.pass_context
def interactive(ctx):
    """Start interactive mode (original request_replayer behavior)."""
    try:
        # Import and run the original interactive menu
        from integration_tools.legacy.request_replayer import interactive_menu
        interactive_menu()
    except Exception as e:
        rprint(f"[red]Error:[/red] {error_handler.handle_general_error(e, 'interactive mode')}")


def main():
    """Main entry point for enhanced request replayer."""
    cli()


if __name__ == "__main__":
    main()