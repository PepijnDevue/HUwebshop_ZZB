# Structured Programming FA3

This code use a rule-based system to create some information in the relational database created in FA2 that can later be used to recommend products either based on a content filter or a collaborative filter.

## Table of Contents (Optional)

- [Usage](#usage)
- [Explanation](#explanation)
- [Contact](#contact)

## Usage

First run add_to_pg.py and then you can import fetch_products as you like to use a product or profile id to get 4 recommendable products. fetch_from_pg.py is an example of how this could be done.

## Explanation

### content filter
The content filter looks at the target_group of a specific product and recommends 4 *other* products that can be found most in the table prev_recommended. If there are less than 4 products in this table from this group, other products that are most previously recommended are added to the output list.

### collaborative filter
The collaborative filter looks at all sessions the profile has done before and what the preference_category was from each of those sessions to find one category that the user likes the most. From this category 4 products are taken that have been recommended most. And here again, if there are less than 4 products in this table from this category, other products that are most previously recommended are added to the output list.

## Contact

For questions please [email me](mailto:pepijn.devue@student.hu.nl)

