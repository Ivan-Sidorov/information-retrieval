import glob
import io
import os
import re
import shutil
import string
import zipfile
from pathlib import Path
from typing import List, Set

import requests
import tqdm
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 YaBrowser/23.11.0.0 Safari/537.36",
    "Content-Type": "text/html",
}


class RoyalLibParser:
    def __init__(self, corpus_size: int) -> None:
        self.corpus_max_size = corpus_size
        self.corpus = []
        self.corpus_size = 0
        self.authors_link = lambda letter: f"https://royallib.com/authors-{letter}.html"

    @staticmethod
    def _get_authors(url: str) -> List[str]:
        response = requests.get(url=url, headers=HEADERS)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        tags = soup.find_all("a")
        links = []
        for tag in tags:
            link = tag.get("href")
            if link:
                links.append("https:" + link)

        return links

    @staticmethod
    def _get_books(url: str) -> Set[str]:
        try:
            response = requests.get(url=url, headers=HEADERS)
        except requests.exceptions.InvalidURL:
            return set([])
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return set([])
        rows = table.findAll("tr")
        links = []
        for tr in rows:
            cols = tr.findAll("td")
            for td in cols:
                tags = td.findAll("a")
                for tag in tags:
                    link = tag.get("href")
                    if link and ("book" in link):
                        link = link.replace("book", "get/html")
                        link = link.split(".html")[0] + ".zip"
                        links.append("https:" + link)
        return set(links)

    @staticmethod
    def _download_html_file(zip_url: str) -> int:
        response = requests.get(url=zip_url, headers=HEADERS, stream=True)
        try:
            zip = zipfile.ZipFile(io.BytesIO(response.content))
            zip.extractall("htmls")
        except zipfile.BadZipFile:
            return 0

        for file in glob.glob("htmls/*"):
            file = Path(file)
            if file.suffix != ".html":
                try:
                    os.remove(file)
                except PermissionError:
                    shutil.rmtree(file)
                    return 0

        return 1

    @staticmethod
    def _has_cyrillic(text):
        return bool(re.search(r"^[а-яёА-ЯЁ]+$", text))

    @staticmethod
    def parse_html(html_path: str) -> List[str]:
        with open(html_path, "rb") as f:
            html = f.read().decode("cp1251")

        corpus = []
        size = 0
        soup = BeautifulSoup(html, "html.parser")
        for div in soup.find_all("div"):
            subdivs = div.find_all("div")
            if (not subdivs) or (
                len(div.find_all("div", attrs={"align": "center"})) == len(subdivs)
            ):
                text = div.get_text()
                text_len = len(text.split(" "))
                if text and (text_len < 10_000) and (text_len > 100):
                    corpus.append(text.strip())
                    size += text_len

        return corpus, size

    def parse_corpus(self) -> Set[str]:
        if os.path.exists("htmls"):
            shutil.rmtree("htmls")
        os.mkdir("htmls")
        self._parse_corpus()
        os.rmdir("htmls")

    def _parse_corpus(self) -> None:
        pbar = tqdm.tqdm(total=self.corpus_max_size)
        for letter in string.ascii_lowercase:
            authors = self._get_authors(self.authors_link(letter))
            for author in authors:
                books = self._get_books(author)
                for book in books:
                    status = self._download_html_file(book)
                    if status:
                        for file in glob.glob("htmls/*"):
                            book_corpus, book_length = self.parse_html(file)
                            self.corpus_size += book_length
                            self.corpus.extend(book_corpus)
                            os.remove(file)

                            pbar.n = self.corpus_size
                            pbar.refresh()

                            if self.corpus_size > self.corpus_max_size:
                                return

    def save_corpus(self, save_path: str) -> None:
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.mkdir(save_path)

        for i, doc in enumerate(self.corpus):
            with open(os.path.join(save_path, f"{i}.txt"), "w") as f:
                f.write(doc)


if __name__ == "__main__":
    parser = RoyalLibParser(corpus_size=10_000_000)
    parser.parse_corpus()
    parser.save_corpus("corpus")
