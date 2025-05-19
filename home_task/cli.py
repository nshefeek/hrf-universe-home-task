import typer

from .services import calculate_and_save_stats_in_batches

cli = typer.Typer()


@cli.command()
def run(
    min_postings_threshold: int = typer.Option(
        5,
        "--min-postings-threshold",
        "-m",
        help="Minimum number of postings to calculate statistics.",
    ),
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
        calculate_and_save_stats_in_batches(min_postings_threshold, batch_size)
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)
    

if __name__ == "__main__":
    cli()