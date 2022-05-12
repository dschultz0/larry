from typing import Optional, Sequence
import unicodedata
from collections import defaultdict
from itertools import chain
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.layout import LAParams, LTTextLine
from pdfminer.converter import PDFLayoutAnalyzer
from pdfminer.pdftypes import PDFStream
from pdfminer.pdffont import PDFFont, PDFUnicodeNotDefined
from pdfminer.pdfcolor import PDFColorSpace
from pdfminer.pdfinterp import PDFGraphicState
from pdfminer.utils import PathSegment, Matrix
from pdfminer.layout import LTPage, LTItem, LTContainer, LTChar

from larry.types import Box, Page, PageList
from larry.utils.image import intersection_over_union


class TextBoxConverter(PDFLayoutAnalyzer):
    def __init__(
            self,
            rsrcmgr: PDFResourceManager,
            pageno: int = 1,
            laparams: Optional[LAParams] = None,
            normalization_form: str = None,
            trim_whitespace: bool = False,
            filter_empty: bool = True,
            exclude_duplicate_characters: bool = True,
            min_duplicate_character_iou: float = 0.99
    ) -> None:
        PDFLayoutAnalyzer.__init__(self, rsrcmgr, pageno=pageno, laparams=laparams)
        self.normalization_form = normalization_form
        self.trim_whitespace = trim_whitespace
        self.filter_empty = filter_empty
        self.pages: PageList = PageList()
        self.exclude_duplicate_characters = exclude_duplicate_characters
        self.min_duplicate_character_iou = min_duplicate_character_iou

    def record_undefined_cid(self, font_name, cid):
        undef_ids = self.pages.get("undefined_character_ids", defaultdict(set))
        self.pages["undefined_character_ids"] = undef_ids
        undef_ids[font_name].add(cid)

    def render_char(
        self,
        matrix: Matrix,
        font: PDFFont,
        fontsize: float,
        scaling: float,
        rise: float,
        cid: int,
        ncs: PDFColorSpace,
        graphicstate: PDFGraphicState,
    ) -> float:
        """
        Overrides the version from PDFLayoutAnalyser with the addition of a step to capture data on any
        cid values that aren't correctly assigned.
        """
        try:
            text = font.to_unichr(cid)
            if text == "\x00":
                self.record_undefined_cid(font.fontname, cid)
            assert isinstance(text, str), str(type(text))
        except PDFUnicodeNotDefined:
            text = self.handle_undefined_char(font, cid)
            self.record_undefined_cid(font.fontname, cid)
        text_width = font.char_width(cid)
        text_disp = font.char_disp(cid)
        item = LTChar(
            matrix,
            font,
            fontsize,
            scaling,
            rise,
            text,
            text_width,
            text_disp,
            ncs,
            graphicstate,
        )
        self.cur_item.add(item)
        return item.adv

    def __char_in_list(self, char: LTChar, arr: [LTItem]):
        return any([intersection_over_union(obj.bbox, char.bbox) > self.min_duplicate_character_iou and
                    obj.get_text() == char.get_text() for obj in arr if isinstance(obj, LTChar)])

    def receive_layout(self, ltpage: LTPage) -> None:
        def render(item: LTItem) -> None:
            if isinstance(item, LTTextLine):
                # This bit of logic is designed to catch cases where the PDF was generated with overlapping,
                # duplicate, text. This is usually a minor issue in the source file that carried into the PDF
                if self.exclude_duplicate_characters:
                    deduped = []
                    [deduped.append(obj) for obj in item if
                     not isinstance(obj, LTChar) or not self.__char_in_list(obj, deduped)]
                    item._objs = deduped
                text = item.get_text()
                if self.normalization_form:
                    text = unicodedata.normalize(self.normalization_form, text)
                if self.trim_whitespace:
                    text = text.strip()
                if len(text) > 0 or not self.filter_empty:
                    boxes.append(
                        Box.from_coordinates(
                            item.bbox,
                            top_origin=False,
                            height=ltpage.height,
                            text=text,
                            fonts=set(char.fontname for char in item if hasattr(char, "fontname"))
                        )
                    )
            elif isinstance(item, LTContainer):
                for child in item:
                    render(child)

        boxes = []
        render(ltpage)
        self.pages.append(Page(ltpage.width, ltpage.height, contents=sorted(boxes), identifier=ltpage.pageid,
                               fonts=set(chain(*[box.fonts for box in boxes if hasattr(box, "fonts")]))))
        self.pages["fonts"] = set(chain(*[p.fonts for p in self.pages]))

    def get_pages(self) -> PageList:
        if len(self.pages) == 0:
            raise Exception("No pages have been processed")
        return self.pages

    def get_boxes(self) -> list[Box]:
        if len(self.pages) == 0:
            raise Exception("No pages have been processed")
        return [box.with_attributes({"page_identifier": page.identifier,
                                     "page_index": i}) for i, page in enumerate(self.pages) for box in page.contents]

    # Some dummy functions to save memory/CPU when all that is wanted
    # is text.  This stops all the image and drawing output from being
    # recorded and taking up RAM.
    def render_image(self, name: str, stream: PDFStream) -> None:
        return

    def paint_path(
            self,
            gstate: PDFGraphicState,
            stroke: bool,
            fill: bool,
            evenodd: bool,
            path: Sequence[PathSegment],
    ) -> None:
        return
