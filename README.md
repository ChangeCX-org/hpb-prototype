# hpb-prototype
## Repository for HPB related experiments.
<br> The module helps building Title similarity using NLP(Natural Language Processing's) Cosine Similarity. (https://en.wikipedia.org/wiki/Cosine_similarity)
<br> The module uses Spacy (https://spacy.io/) and en_core_web_lg package (For Language Corpus)
## Installation and Usage
### Installing PySpark
#### pip install pyspark
### Installing Spacy
#### pip install spacy
## To Run
<ul> 
  <li> Ensure the file curated_book_short.csv is created which is of format csv and has following headers (id,title,author,author_id,author_bio,authors,title_slug,author_slug,isbn13,isbn10,price,format,publisher,pubdate,edition,subjects,pages,dimensions) </li>
  <li>cd src/ml-experiements</li>
  <li> py GroupTitlesBySimilarity.py</li>
</ul>
