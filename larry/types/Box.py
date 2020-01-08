import collections
import larry


class Box(collections.UserDict):
    def __init__(self, *data):
        collections.UserDict.__init__(self)
        for dat in data:
            self.update(dat)

    @classmethod
    def from_position(cls, position, calc_coordinates=False, **kwargs):
        obj = cls(position, kwargs)
        if calc_coordinates:
            obj._calc_coordinates()
        return obj

    @classmethod
    def from_coords(cls, coordinates, calc_position=False, origin='topleft', size=None, width=None, height=None,
                    **kwargs):
        if origin == 'bottomleft':
            if size is None and height is None:
                raise Exception("Using an origin of 'bottomleft' requires a size or height be provided")
            (width, height) = size if size else (None, height)
            coordinates = [coordinates[0], height - coordinates[3], coordinates[2], height - coordinates[1]]
        obj = cls({'coordinates': coordinates}, kwargs)
        if calc_position:
            obj._calc_position()
        return obj

    def scaled(self, ratio):
        box = self.copy()
        for k, v in box.items():
            if k == 'coordinates':
                box[k] = [v[0] * ratio, v[1] * ratio, v[2] * ratio, v[3] * ratio]
            elif k in ['top', 'left', 'width', 'height']:
                box[k] = v * ratio
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

    def intersecting_boxes(self, boxes):
        intersecting = []
        for box in boxes:
            if larry.utils.image.box_area(larry.utils.image.box_intersection(self, box)) > 0:
                intersecting.append(box.offset(-self['left'], -self['top']))
        return intersecting

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