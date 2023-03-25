# Structured Programming FA3

This code use a rule-based system to create some information in the relational database created in FA2 that can later be used to recommend products either based on a content filter or a collaborative filter.

## Table of Contents (Optional)

- [Usage](#usage)
- [Explanation](#explanation)
- [AR](#ar)
- [Contact](#contact)

## Usage

First run add_to_pg.py and then you can import fetch_products as you like to use a product or profile id to get 4 recommendable products. fetch_from_pg.py is an example of how this could be done.

## Explanation

### content filter
The content filter looks at the target_group of a specific product and recommends 4 *other* products that can be found most in the table prev_recommended. If there are less than 4 products in this table from this group, other products that are most previously recommended are added to the output list.

### collaborative filter
The collaborative filter looks at all sessions the profile has done before and what the preference_category was from each of those sessions to find one category that the user likes the most. From this category 4 products are taken that have been recommended most. And here again, if there are less than 4 products in this table from this category, other products that are most previously recommended are added to the output list.

## AR

This paragraph will contain the theoretical notation learned in AR for the data-collection for the new tables and rows created *highlighted green in the [Entity Relation Diagram](./ERD.png)*

### Most_recommended

```
P = verzameling van alle producten
R = verzameling van alle keren dat een product is aangeraden
n = #(R)

F(p) = ∑_{i=1}^{n} (p∈R_i)
Voor product p, tel alle keren dat het is aangeraden

∀x ∈ P : F(x)
Voor alle producten in P, tel het aantal keer dat dat product is aangeraden
```

### Top_category

```
Prof = verzameling van alle profielen
S = verzameling van alle sessions
Prod = verzameling van alle producten
C = {category|category ∈ Prod}
n = #(S)

FreqCat(p, c) = ∑_{i=1}^{n} (S_i.buid = p.buid ∧ S_i.preference_category = c)
tel alle sessions bij elkaar op waar de buid matcht met die van de ingevoerde profile en de preference_category matcht met de ingevoerd categorie

PrefCats(p) = { FreqCat(p, c) | c ∈ C}
Maak een frequentietabel voor één profiel met daarin alle categorieen en hoevaak die prefered zijn in sessies van dat profiel

PrefCat(p) = (c ∈ PrefCats(p) | ∀x ∈ PrefCats(p) : c.frequentie ≥ x.frequentie)
Selecteer de categorie uit de verzameling die PrefCats teruggeeft die de hoogste frequentie heeft

∀p ∈ Prof : PrefCat(p)
Voor alle profielen, pak de categorie die het meest is gepreferreerd in de bijbehorende sessies
```


## Contact

For questions please [email me](mailto:pepijn.devue@student.hu.nl)

