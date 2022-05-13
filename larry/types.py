from collections.abc import Mapping
import warnings
from typing import Dict


class ClientError(Exception):
    """
    Wraps a boto ClientException to make it easier to use. All of the attributes of the source exception are
    accessible with the exception of the traceback which is truncated to actions within larry. This cuts down
    on the need to review the long stack trace that boto3 produces for errors that happen on the service. In
    addition, the error *code* and *message* are accessible as properties.
    """

    def __init__(self, error):
        self.__error = error

    @classmethod
    def from_boto(cls, error):
        """
        Captures the exception returned by Boto and truncates the traceback to stop at the last *larry* operation.
        """
        tb = error.__traceback__
        larry_found = 'larry' in tb.tb_frame.f_code.co_filename
        while tb.tb_next:
            if 'larry' in tb.tb_next.tb_frame.f_code.co_filename:
                larry_found = True
            elif larry_found:
                tb.tb_next = None
                break
            tb = tb.tb_next
        return cls(error).with_traceback(tb)

    def __getattr__(self, name):
        if hasattr(self.__error, name):
            return getattr(self.__error, name)
        else:
            super().__getattribute__(self, name)

    @property
    def code(self):
        """
        Surfaces the error code that was returned from the AWS service.

        .. code-block:: python

            try:
                data = lry.s3.read_as(str, 's3://my-bucket/does_not_exist.txt')
            except lry.ClientException as e:
                if e.code == '404':
                    #do something
                elif e.code == lry.s3.error_codes.AccessDenied:
                    #do something else

        :return: A string containing the error code returned from the service.
        """
        return self.__error.response['Error']['Code']

    @property
    def message(self):
        """
        Surfaces the error message that was returned from the AWS service.

        :return: A string containing the error message.
        """
        return self.__error.response['Error']['Message']


class AttrObject:
    _attributes = None

    def __init__(self, attributes: Dict = None, **kwargs):
        self._attributes = attributes
        if kwargs:
            if self._attributes is None:
                self._attributes = kwargs
            else:
                self._attributes.update(kwargs)

    @property
    def attributes(self):
        return self._attributes

    def __getattr__(self, item):
        if self._attributes and item in self._attributes:
            return self._attributes[item]
        raise AttributeError(f"AttributeError: '{self.__class__.__name__}' object has no attribute '{item}'")

    def __getitem__(self, item):
        if self._attributes and item in self._attributes:
            return self._attributes[item]
        raise KeyError(f"KeyError: '{item}'")

    def __contains__(self, item):
        return item in self._attributes if self._attributes else False

    def get(self, key, default=None):
        if self._attributes and key in self._attributes:
            return self._attributes[key]
        return default

    def __setitem__(self, key, value):
        if self._attributes is None:
            self._attributes = {}
        self._attributes[key] = value

    def __delattr__(self, item):
        if item == "attributes":
            self._attributes = None
        elif self._attributes:
            del self._attributes[item]
        else:
            raise KeyError(f"KeyError: '{item}'")

    def __delitem__(self, key):
        if self._attributes:
            del self._attributes[key]
        else:
            raise KeyError(f"KeyError: '{key}'")

    def pop(self, item):
        if self._attributes is None:
            raise KeyError(f"KeyError: '{item}'")
        return self._attributes.pop(item)

    @property
    def data(self):
        return self._attributes if self._attributes else {}

    def to_dict(self):
        return self.data


class Box(AttrObject):
    """
    A representation of a box that can handle the distinct coordinate systems used by the different AWS
    services, as well as various libraries. This provides a mechanism for moving between different systems and
    enables basic operations on the boxes.

    A Box can be created from two distinct coordinate systems:

    * **coordinates**: `[x1, y1, x2, y2]`
    * a dict containing the **position** defined by top, left, width, height

    Box itself represents its coordinates in pixels that are measured from the top-left. To support a range of options
    the class methods can also source data that has a bottom-left origin or is defined as a ratio of the image size.

    .. code-block:: python

        from larry.types import Box
        from larry.utils import JSONEncoder
        import json


        tlwh = { 'top': 10, 'left': 10, 'width': 30, 'height': 30 }
        coords = [10, 10, 40, 40]
        coords_ratio = [0.1, 0.1, 0.4, 0.4]
        obj = {
            'coordinates': [10, 10, 40, 40],
            'label': 'green',
            'name': 'my box'
        }

        box = Box.from_position(tlwh)
        box = Box.from_coords(coords)
        box = Box.from_position_ratio(coords_ratio, width=100, height=100)
        box = Box.from_position_ratio(coords_ratio, top_origin=False, size=(100,100))
        box = Box.from_dict(obj)

        # writes out the JSON for the obj value above, supplemented with top, left, width,
        # and height attributes
        print(json.dumps(box, cls=JSONEncoder)
    """
    MAX_SCALE = 6

    def __init__(self, value, attributes: Dict = None, **kwargs):
        attr = None
        if isinstance(value, Box):
            self._coordinates = tuple(value.coordinates)
            attr = value.attributes.copy() if value.attributes else None
        elif isinstance(value, list) or isinstance(value, tuple):
            self._coordinates = tuple(value)
            attr = attributes
        elif isinstance(value, Dict):
            value = value.copy()
            self._coordinates = self.__case_insensitive_pop(value, "coordinates", raise_if_missing=False)
            if not self._coordinates:
                self._coordinates = self.position_to_coordinates(
                    self.__case_insensitive_pop(value, "left"),
                    self.__case_insensitive_pop(value, "top"),
                    self.__case_insensitive_pop(value, "width"),
                    self.__case_insensitive_pop(value, "height")
                )
            if attributes:
                value.update(attributes)
            attr = {k: v for k, v in value.items()
                    if k.lower() not in ["coordinates", "left", "top", "width", "height", "__box__"]}
        if len(self._coordinates) != 4:
            raise ValueError("Box coordinates must have exactly four values")
        self._coordinates = [round(c, self.MAX_SCALE) for c in self._coordinates]
        AttrObject.__init__(self, attr, **kwargs)

    @property
    def coordinates(self):
        return self._coordinates

    @property
    def left(self):
        return self._coordinates[0]

    @property
    def top(self):
        return self._coordinates[1]

    @property
    def width(self):
        return round(self._coordinates[2] - self._coordinates[0], self.MAX_SCALE)

    @property
    def height(self):
        return round(self._coordinates[3] - self._coordinates[1], self.MAX_SCALE)

    @property
    def bottom(self):
        return self._coordinates[3]

    @property
    def right(self):
        return self._coordinates[2]

    @property
    def area(self):
        """
        Returns the area of the Box in square pixels
        """
        return abs(self)

    def copy(self, location_only=False):
        return Box(tuple(self._coordinates),
                   self._attributes.copy() if self._attributes and not location_only else None)

    def with_attributes(self, additional_attributes: dict):
        attr = self._attributes.copy()
        attr.update(additional_attributes)
        return Box(tuple(self._coordinates), attr)

    @classmethod
    def from_dict(cls, obj):
        """
        Creates a Box object containing contents of the dict that is passed. It's expected that
        the dict passed contains either position information as top/left/width/height elements or
        an element containing coordinates.

        :param obj: The source dict to build the Box from
        :return: A Box object
        """
        return cls(obj)

    @staticmethod
    def __case_insensitive_pop(d, key, default=None, raise_if_missing=True):
        if key in d:
            return d.pop(key)
        else:
            for k in d.keys():
                if k.lower() == key.lower():
                    return d.pop(k)
        if raise_if_missing:
            raise KeyError(f'Value for {key} is missing')
        else:
            return default

    @classmethod
    def from_position(cls, position, **kwargs):
        """
        Creates a Box object containing contents of the dict that is passed as a position.

        :param position: The source dict to build the Box from
        :param kwargs: Additional keyword attributes that should be included in the Box attributes
        :return: A Box object
        """
        return cls(position, kwargs)

    @classmethod
    def from_position_ratio(cls, position, size=None, width=None, height=None, scale=1, **kwargs):
        """
        Creates a Box object containing contents of the dict that is passed as a position that is defined as
        a share of the image size.

        :param position: The source dict to build the Box from
        :param size: The size of the image as a tuple (width, height)
        :param width: The width of the image in pixels
        :param height: The height of the image in pixels
        :param scale: The number of digits to the right of the decimal point to include in the calculated coordinates
        :param kwargs: Additional keyword attributes that should be included in the Box attributes
        :return: A Box object
        """
        (width, height) = size if size else (width, height)
        if width is None or height is None:
            raise ValueError('Image dimensions must be provided')
        p = position.copy()
        coordinates = cls.position_to_coordinates(round(cls.__case_insensitive_pop(p, "left") * width, scale),
                                                  round(cls.__case_insensitive_pop(p, "top") * height, scale),
                                                  round(cls.__case_insensitive_pop(p, "width") * width, scale),
                                                  round(cls.__case_insensitive_pop(p, "height") * height, scale))
        if kwargs:
            p.update(kwargs)
        return cls(coordinates, p)

    @staticmethod
    def __normalize_bottom_origin(coordinates, height):
        if height is None:
            raise TypeError("Image height must be provided when the origin is bottom-left")
        return [coordinates[0], height - coordinates[3], coordinates[2], height - coordinates[1]]

    @classmethod
    def from_coordinates(cls, coordinates, top_origin=True, height=None, **kwargs):
        """
        Creates a Box object using coordinates of the box in pixels.

        :param coordinates: The coordinates of the box (x1, y1, x2, y2)
        :param top_origin: Indicates the origin from which the coordinates are calculated, False if bottom origin
        :param height: The height of the image in pixels (required for bottom origin coordinates)
        :param kwargs: Additional keyword attributes that should be included in the Box attributes
        :return: A Box object
        """
        if len(coordinates) != 4:
            raise TypeError('Coordinates must have exactly four values')
        if not top_origin:
            coordinates = cls.__normalize_bottom_origin(coordinates, height)
        return cls(coordinates, kwargs)

    @classmethod
    def from_coordinates_ratio(cls, coordinates, top_origin=True, size=None, width=None, height=None, **kwargs):
        """
        Creates a Box object using coordinates of the box in pixels.

        :param coordinates: The coordinates of the box (x1, y1, x2, y2)
        :param top_origin: Indicates the origin from which the coordinates are calculated, False if bottom origin
        :param size: The size of the image as a tuple (width, height)
        :param width: The width of the image in pixels
        :param height: The height of the image in pixels
        :param kwargs: Additional keyword attributes that should be included in the attributes
        :return: A Box object
        """
        (width, height) = size if size else (width, height)
        if len(coordinates) != 4:
            raise TypeError('Coordinates must have exactly four values')
        if width is None or height is None:
            raise TypeError('Image dimensions must be provided if the coordinates are defined as a ratio')
        coordinates[0] = width * coordinates[0]
        coordinates[1] = height * coordinates[1]
        coordinates[2] = width * coordinates[2]
        coordinates[3] = height * coordinates[3]
        if not top_origin:
            coordinates = cls.__normalize_bottom_origin(coordinates, height)
        return cls(coordinates, kwargs)

    @staticmethod
    def position_to_coordinates(left, top, width, height):
        return [left, top, left + width, top + height]

    @staticmethod
    def is_box(obj):
        """
        Returns true or false if the dict that is provided contains the keys or coordinates necessary to be a box
        in the pattern required by this class.

        :param obj: The dict or array to check
        """
        return (isinstance(obj, Mapping) and (('top' in obj and 'left' in obj and 'width' in obj and 'height' in obj) or
                                              ('coordinates' in obj and len(obj['coordinates']) == 4))) or (
                       (isinstance(obj, list) or isinstance(obj, tuple)) and len(obj) == 4
               )

    def __abs__(self):
        return self.width * self.height

    def __mul__(self, scalar):
        return Box([x * scalar for x in self.coordinates], self.attributes)

    def __add__(self, other):
        # TODO: Consider how to combine attributes if at all
        if isinstance(other, Box):
            other = other.coordinates
        if isinstance(other, list) or isinstance(other, tuple):
            if len(other) == 4:
                pairs = list(zip(self.coordinates, other))
                return Box([min(pairs[0]), min(pairs[1]), max(pairs[2]), max(pairs[3])])
            elif len(other) == 2:
                c = self.coordinates
                x = other[0]
                y = other[1]
                return Box([c[0] + x, c[1] + y, c[2] + x, c[3] + y], self._attributes)
        raise ValueError("Invalid value to add to a Box")

    def __sub__(self, other):
        if (isinstance(other, list) or isinstance(other, tuple)) and len(other) == 2:
            c = self.coordinates
            x = other[0]
            y = other[1]
            return Box([c[0] - x, c[1] - y, c[2] - x, c[3] - y], self._attributes)
        raise ValueError("Invalid value to subtract from a Box")

    def __gt__(self, other):
        return self.top > other.top

    def __lt__(self, other):
        return self.top < other.top

    def __radd__(self, other):
        return self.copy(True) if other == 0 else self + other

    def intersecting_boxes(self, boxes, min_overlap=0):
        return [box for box in boxes if self & box and abs(self & box) > abs(box) * min_overlap]

    def __and__(self, other):
        a = self.coordinates
        b = other.coordinates
        intersection = max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])
        if intersection[2] < intersection[0] or intersection[3] < intersection[1]:
            return None
        else:
            return Box(intersection)

    def __round__(self, n=None):
        return Box([round(x, n) for x in self.coordinates], self.attributes)

    def __repr__(self):
        if self.attributes:
            return f"Box({list(self.coordinates)}, {repr(self.attributes)})"
        else:
            return f"Box({list(self.coordinates)})"

    def scaled(self, ratio):
        """
        Creates a new instance of the Box that has been scaled based on the provided ratio (up or down).
        DEPRECATED in favor of simply using a multiplication operation

        :param ratio: The ratio to scale the box; a value less than 1 would scale it down, greater would scale it up
        :return: A Box object
        """
        warnings.warn("Use the multiplication operator", DeprecationWarning)
        return self * ratio

    def offset(self, x, y):
        """
        Creates a new instance of the Box that has been offset by the number of pixels in x and y.
        DEPRECATED in favor of simply using an addition operation

        :param x: The number of pixels to offset in the horizontal dimension
        :param y: The number of pixels to offset in the vertical dimension
        :return: A Box object
        """
        warnings.warn("Use the addition operator", DeprecationWarning)
        return self + [x, y]

    @property
    def data(self):
        d = self._attributes if self._attributes else {}
        d.update({
            "coordinates": list(self._coordinates),
            "left": self.left,
            "top": self.top,
            "width": self.width,
            "height": self.height
        })
        return d


class Page(AttrObject):
    """
    A representation of a "page" of shape elements such as Box and the relevant information about the page
    source to support operations on the contents.
    """
    def __init__(self, width: float, height: float, contents: [Box] = None, index: int = None, identifier=None,
                 attributes: Dict = None, **kwargs):
        self._width = width
        self._height = height
        self._contents = contents
        self._index = index
        self._identifier = identifier
        AttrObject.__init__(self, attributes, **kwargs)

    @property
    def width(self) -> float:
        return self._width

    @property
    def height(self) -> float:
        return self._height

    @property
    def size(self) -> []:
        return [self._width, self._height]

    @property
    def contents(self) -> [Box]:
        return self._contents

    @property
    def index(self) -> int:
        return self._index

    @property
    def identifier(self):
        return self._identifier

    def __repr__(self):
        first_line = "<Page "
        if self._identifier:
            first_line += f"{self._identifier} "
        if self._contents:
            first_line += "containing {len(self._contents)} items"
        lines = [
            first_line,
            f"  - width: {self._width}",
            f"  - height: {self._height}",
        ]
        if self.attributes:
            for k, v in self.attributes.items():
                lines.append(f"  - {k}: {v}")
        if self._contents:
            lines.append("  - contents:")
            lines.extend(["    " + repr(b) for b in self._contents[:10]])
            if len(self._contents) > 10:
                lines.append("    ...")
        lines.append(">")
        return "\n".join(lines)


class PageList(AttrObject):
    """
    A set of Page objects
    """
    def __init__(self, pages: [Page] = None, vertical: bool = True, attributes: Dict = None, **kwargs):
        self._pages = pages if pages else []
        self._vertical = vertical
        AttrObject.__init__(self, attributes, **kwargs)

    @classmethod
    def from_indices(cls, indices: list[list]):
        # TODO: Fix the class so that it handles scenarios with indexes that don't abut each other
        return cls([Page(ind[2]-ind[0], ind[3]-ind[1], index=i, identifier=i) for i, ind in enumerate(indices)])

    def __len__(self):
        return len(self._pages)

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self) -> Page:
        if self.n < len(self._pages):
            result = self._pages[self.n]
            self.n += 1
            return result
        else:
            raise StopIteration

    def __getitem__(self, item):
        return self._pages[item]

    def append(self, page: Page):
        self._pages.append(page)

    @property
    def vertical(self):
        return self._vertical

    @property
    def width(self):
        if self._vertical:
            return max([page.width for page in self._pages])
        else:
            return sum([page.width for page in self._pages])

    @property
    def height(self):
        if self._vertical:
            return sum([page.height for page in self._pages])
        else:
            return max([page.height for page in self._pages])

    @property
    def page_offsets(self) -> [[float, float]]:
        offsets = []
        index = 0
        for page in self._pages:
            if self._vertical:
                offsets.append([0, index])
                index += page.height
            else:
                offsets.append([index, 0])
                index += page.width
        return offsets

    @property
    def page_indices(self) -> [[float, float, float, float]]:
        return [[indices[0],
                 indices[1],
                 indices[0] + page.width,
                 indices[1] + page.height] for page, indices in zip(self, self.page_offsets)]

    def consolidate_content(self, target, max_dimension_variance: float = 0.01):
        if len(self) != len(target):
            raise Exception("Page counts don't match")
        if self._vertical != target.vertical:
            raise Exception("The target orientation doesn't match")

        # Adjust the coordinates to align with the target
        # Scale the content to match the dimensions of the target
        width_ratio = target.width / self.width
        height_ratio = target.height / self.height

        # Confirm the ratio of width and height are reasonably close
        if abs(width_ratio / height_ratio - 1) > max_dimension_variance:
            raise Exception(f"Mismatched width and height ratios: ({width_ratio}, {height_ratio})")

        return [
            (box * height_ratio) + offset
            for page, offset in zip(self, target.page_offsets)
            for box in page.contents
        ]

    def __repr__(self):
        lines = [
            f"<PageSet containing {len(self._pages)} pages",
            f"  - width: {self.width}",
            f"  - height: {self.height}",
        ]
        if self.attributes:
            for k, v in self.attributes.items():
                lines.append(f"  - {k}: {v}")
        if self._pages:
            lines.append("  - pages:")
            lines.extend(["    " + s for p in self._pages for s in repr(p).split("\n")])
        lines.append(">")
        return "\n".join(lines)
