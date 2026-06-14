WORKS_DATASETS = (
    "works",
    "citation_edges",
    "authorships",
    "affiliations",
    "institutions",
    "sources",
    "concepts",
    "scores",
    "selected_scores"
)


def works_output_paths(shard_name, output_root):
    return {
        dataset: output_root / dataset / f"{shard_name}.parquet"
        for dataset in WORKS_DATASETS
    }


def authors_output_path(shard_name, output_root):
    return output_root / "authors" / f"{shard_name}.parquet"