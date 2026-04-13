# wadoh_raccoon

A Python package for transforming and linking pathogen sequencing/subtyping metadata.

See our github page for function references and docs https://nw-page.github.io/wadoh_raccoon/ 

## Install

```bash
uv add wadoh_raccoon
```
or 

```bash
uv pip install wadoh_raccoon
```

## Usage

See [Reference](https://nw-page.github.io/wadoh_raccoon/reference/) for more details

For fuzzy matching:

```python
from wadoh_raccoon import dataframe_matcher as dfm

fuzzy_init = dfm.DataFrameMatcher(
    df_src=your_df,
    df_ref=reference_df,
    first_name=('first_name', 'first_name_reference'),
    last_name=('last_name', 'last_name_reference'),
    dob='birth_date',
    spec_col_date=('sub_collection_date', 'ref_collection_date'),
    key='submission_number',
    threshold=80  # set what kind of fuzzy threshold you want, 100 being exact match
)

```

```python
result = fuzzy_init.match()
```

```python
# records that matched
result.fuzzy_matched

# records that didn't match
result.fuzzy_unmatched


```

