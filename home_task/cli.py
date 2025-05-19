import typer

from .services import calculate_and_save_stats_in_batches

cli = typer.Typer()


@cli.command()
def run(
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        "-b",
        help="Batch size for processing job postings.",
    ),
) -> None:

    """
    Calculate statistics for job postings.
    """
    typer.echo("Calculating statistics...")


    try:
        typer.echo("Fetching standard job IDs from source table..")
        calculate_and_save_stats_in_batches(batch_size)
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)
    

if __name__ == "__main__":
    cli()