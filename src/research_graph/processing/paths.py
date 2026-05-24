WORKS_DATASETS = (
    "works",
    "citation_edges",
    "authorships",
    "affiliations",
    "institutions",
    "venues",
    "concepts",
    "scores",
    "selected_scores"
)


def works_output_paths(shard_name, data_root):
    return {
        dataset: data_root / dataset / f"{shard_name}.parquet"
        for dataset in WORKS_DATASETS
    }


def authors_output_path(shard_name, data_root):
    return data_root / "authors" / f"{shard_name}.parquet"