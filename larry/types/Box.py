from collections import UserDict, Mapping
from larry.utils import image


class Box(UserDict):
    def __init__(self, *data):
        UserDict.__init__(self)
        for dat in data:
            self.update(dat)

    @classmethod
    def from_obj(cls, obj):
        result = cls(obj)
        if 'top' in obj and 'left' in obj and 'width' in obj and 'height' in obj:
            result._calc_coordinates()
        elif 'coordinates' in obj and len(obj['coordinates']) == 4:
            result._calc_position()
        return result

    @classmethod
    def from_position(cls, position, **kwargs):
        obj = cls(position, kwargs)
        obj._calc_coordinates()
        return obj

    @classmethod
    def from_percentage(cls, coordinates, origin='topleft', size=None, width=None, height=None, **kwargs):
        if size is None and (height is None or width is None):
            raise Exception("Size or height and width are required")
        (width, height) = size if size else (width, height)

        if origin == 'bottomleft':
            coordinates = [coordinates[0] * width, (1 - coordinates[1]) * height,
                           coordinates[2] * width, (1 - coordinates[3]) * height]
        else:
            coordinates = [coordinates[0] * width, coordinates[1] * height,
                           coordinates[2] * width, coordinates[3] * height]
        obj = cls({'coordinates': coordinates}, kwargs)
        obj._calc_position()
        return obj

    @classmethod
    def from_coords(cls, coordinates, origin='topleft', size=None, width=None, height=None, **kwargs):
        if origin == 'bottomleft':
            if size is None and height is None:
                raise Exception("Using an origin of 'bottomleft' requires a size or height be provided")
            (width, height) = size if size else (None, height)
            coordinates = [coordinates[0], height - coordinates[3], coordinates[2], height - coordinates[1]]
        obj = cls({'coordinates': coordinates}, kwargs)
        obj._calc_position()
        return obj

    @staticmethod
    def is_box(obj):
        return isinstance(obj, Mapping) and (('top' in obj and 'left' in obj and 'width' in obj and 'height' in obj) or
                                             ('coordinates' in obj and len(obj['coordinates']) == 4))

    def scaled(self, ratio):
        box = self.copy()
        for key, value in box.items():
            if key == 'coordinates':
                box[key] = [v * ratio for v in value]
            elif key in ['top', 'left', 'width', 'height']:
                box[key] = value * ratio
        return box

    def offset(self, x, y):
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
        intersecting = []
        for box in boxes:
            if image.box_area(image.box_intersection(self, box)) > image.box_area(box)*min_overlap:
                intersecting.append(box.offset(-self['left'], -self['top']))
        return intersecting

    def area(self):
        return self['width'] * self['height']

    def _calc_coordinates(self):
        self['coordinates'] = [self['left'], self['top'], self['left'] + self['width'] - 1,
                               self['top'] + self['height'] - 1]

    def _calc_position(self):
        self['left'] = self['coordinates'][0]
        self['top'] = self['coordinates'][1]
        self['width'] = self['coordinates'][2] - self['coordinates'][0] + 1
        self['height'] = self['coordinates'][3] - self['coordinates'][1] + 1

    def __missing__(self, key):
        if key == 'coordinates':
            self._calc_coordinates()
            return self[key]
        elif key in ['top', 'left', 'width', 'height']:
            self._calc_position()
            return self[key]
        else:
            raise KeyError(key)
