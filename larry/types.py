from collections import UserDict, Mapping
from enum import Enum


# Types used in IO operations to designate how data should be written/read
class Types(Enum):
    DICT = 1
    STRING = 2
    PILLOW_IMAGE = 3
    DELIMITED = 4
    JSON_LINES = 5
    NP_ARRAY = 6


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
                data = lry.s3.read_str('s3://my-bucket/does_not_exist.txt')
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


class Box(UserDict):
    """
    A representation of a box that can handle the distinct coordinate systems used by the different AWS
    services, as well as various libraries. This provides a mechanism for moving between different systems and
    enables basic operations on the boxes.

    Because Box extends a UserDict object it will also retain any additional attributes that are provided or assigned.

    A Box can be created from two distinct coordinate systems:

    * **coordinates**: `[x1, y1, x2, y2]`
    * a dict containing the **position** defined by top, left, width, height

    Box itself represents it's coordinates in pixels that are measured from the top-left. To support a range of options
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
        box = Box.from_position(coords_ratio, as_ratio=True, width=100, height=100)
        box = Box.from_position(coords_ratio, as_ratio=True, top_origin=False, size=(100,100))
        box = Box.from_dict(obj)

        # writes out the JSON for the obj value above, supplemented with top, left, width,
        # and height attributes
        print(json.dumps(box, cls=JSONEncoder)
    """

    def __init__(self, *data):
        UserDict.__init__(self)
        for dat in data:
            self.update(dat)

    @classmethod
    def from_dict(cls, obj):
        # TODO: Rework this to call the other class methods depending on what system it contains
        """
        Creates a Box object containing contents of the dict that is passed. It's expected that
        the dict passed contains either position information as top/left/width/height elements or
        an element containing coordinates.

        :param obj: The source dict to build the Box from
        :return: A Box object
        """
        result = cls(obj)
        if 'top' in obj and 'left' in obj and 'width' in obj and 'height' in obj:
            result._calc_coordinates()
        elif 'coordinates' in obj:
            if len(obj['coordinates']) == 4:
                result._calc_position()
            else:
                raise TypeError('Coordinates must have exactly four values')
        else:
            raise TypeError('The source dict must have either a position (top/left/height/width) or coordinates')
        return result

    @classmethod
    def from_position(cls, position, as_ratio=False, size=None, width=None, height=None, **kwargs):
        """
        Creates a Box object containing contents of the dict that is passed as a position.

        :param position: The source dict to build the Box from
        :param as_ratio: Indicates that the values are represented as a share of the image dimensions
        :param size: The size of the image as a tuple (width, height)
        :param width: The width of the image in pixels
        :param height: The height of the image in pixels
        :param kwargs: Additional keyword attributes that should be included in the Box dict
        :return: A Box object
        """
        obj = cls(position, kwargs)
        if 'top' not in obj or 'left' not in obj or 'width' not in obj or 'height' not in obj:
            if 'Top' in obj and 'Left' in obj and 'Width' in obj and 'Height' in obj:
                obj['top'] = obj.pop('Top')
                obj['left'] = obj.pop('Left')
                obj['width'] = obj.pop('Width')
                obj['height'] = obj.pop('Height')
            else:
                raise TypeError('The source dict must contain a valid position containing top, left, width, and height values')
        if as_ratio:
            (width, height) = size if size else (width, height)
            if width is None or height is None:
                raise TypeError('Image dimensions must be provided if the position is defined as a ratio')
            obj['top'] = height * obj['top']
            obj['left'] = width * obj['left']
            obj['height'] = height * obj['height']
            obj['width'] = width * obj['width']
        obj._calc_coordinates()
        return obj

    @classmethod
    def from_coords(cls, coordinates, top_origin=True, as_ratio=False, size=None, width=None, height=None, **kwargs):
        """
        Creates a Box object using coordinates of the box in pixels.

        :param coordinates: The coordinates of the box (x1, y1, x2, y2)
        :param top_origin: Indicates the origin from which the coordinates are calculated, False if bottom origin
        :param as_ratio: Indicates that the values are represented as a share of the image dimensions
        :param size: The size of the image as a tuple (width, height)
        :param width: The width of the image in pixels
        :param height: The height of the image in pixels
        :param kwargs: Additional keyword attributes that should be included in the Box dict
        :return: A Box object
        """
        (width, height) = size if size else (width, height)
        if len(coordinates) != 4:
            raise TypeError('Coordinates must have exactly four values')
        if as_ratio:
            if width is None or height is None:
                raise TypeError('Image dimensions must be provided if the coordinates are defined as a ratio')
            coordinates[0] = width * coordinates[0]
            coordinates[1] = height * coordinates[1]
            coordinates[2] = width * coordinates[2]
            coordinates[3] = height * coordinates[3]
        if not top_origin:
            if height is None:
                raise TypeError("Image height must be provided when the origin is bottom-left")
            coordinates = [coordinates[0], height - coordinates[3], coordinates[2], height - coordinates[1]]
        obj = cls({'coordinates': coordinates}, kwargs)
        obj._calc_position()
        return obj

    @staticmethod
    def is_box(obj):
        """
        Returns true or false if the dict that is provided contains the keys necessary to be a box
        in the pattern required by this class.

        :param obj: The dict to check
        """
        return isinstance(obj, Mapping) and (('top' in obj and 'left' in obj and 'width' in obj and 'height' in obj) or
                                             ('coordinates' in obj and len(obj['coordinates']) == 4))

    def scaled(self, ratio):
        """
        Creates a new instance of the Box that has been scaled based on the provided ratio (up or down).

        :param ratio: The ratio to scale the box; a value less than 1 would scale it down, greater would scale it up
        :return: A Box object
        """
        box = self.copy()
        for key, value in box.items():
            if key == 'coordinates':
                box[key] = [v * ratio for v in value]
            elif key in ['top', 'left', 'width', 'height']:
                box[key] = value * ratio
        return box

    def offset(self, x, y):
        """
        Creates a new instance of the Box that has been offset by the number of pixels in x and y.

        :param x: The number of pixels to offset in the horizontal dimension
        :param y: The number of pixels to offset in the vertical dimension
        :return: A Box object
        """
        box = self.copy()
        for k, v in box.items():
            if k == 'coordinates':
                box[k] = [v[0] + x, v[1] + y, v[2] + x, v[3] + y]
            elif k == 'left':
                box[k] = v + x
            elif k == 'top':
                box[k] = v + y
        return box

    def intersecting_boxes(self, boxes, min_overlap=0):
        from larry.utils import image

        intersecting = []
        for box in boxes:
            if image.box_area(image.box_intersection(self, box)) > image.box_area(box) * min_overlap:
                intersecting.append(box.offset(-self['left'], -self['top']))
        return intersecting

    @property
    def area(self):
        """
        Returns the area of the Box in square pixels
        """
        return self['width'] * self['height']

    @property
    def top(self):
        """
        Returns the top of the box in pixels measured from the top-left.
        """
        return self['top']

    @property
    def left(self):
        """
        Returns the left-most location of the box in pixels measured from the top-left.
        """
        return self['left']

    @property
    def height(self):
        """
        Returns the height of the box in pixels.
        """
        return self['height']

    @property
    def width(self):
        """
        Returns the width of the box in pixels.
        """
        return self['width']

    @property
    def coordinates(self):
        """
        Returns the coordinates (x1, y1, x2, y2) of the box in pixels measured from the top-left.
        """
        return self['coordinates']

    def _calc_coordinates(self):
        self['coordinates'] = [self['left'], self['top'], self['left'] + self['width'],
                               self['top'] + self['height']]

    def _calc_position(self):
        self['left'] = self['coordinates'][0]
        self['top'] = self['coordinates'][1]
        self['width'] = self['coordinates'][2] - self['coordinates'][0]
        self['height'] = self['coordinates'][3] - self['coordinates'][1]

    def __missing__(self, key):
        if key == 'coordinates':
            self._calc_coordinates()
            return self[key]
        elif key in ['top', 'left', 'width', 'height']:
            self._calc_position()
            return self[key]
        else:
            raise KeyError(key)
