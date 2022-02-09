from boilerpy3 import extractors

filename = "./news.txt"
with open(filename, 'r', encoding="utf-8") as file:
    lines = file.read()
    extractor = extractors.ArticleExtractor()
    content = extractor.get_content(lines)
    print(content)