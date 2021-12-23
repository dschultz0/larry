from larry import utils
from larry import mturk
import collections


class HIT(collections.UserDict):

    def __init__(self, data, mturk_client=None, production=None):
        self.__client = mturk_client
        collections.UserDict.__init__(self)
        if isinstance(data, str):
            hit, prod = mturk._get_hit(data, mturk_client)
            self.update(hit)
            self['Production'] = prod
        else:
            self.update(data)
            self._parse_datetime_values()
            if production is not None:
                self['Production'] = production

    def _parse_datetime_values(self):
        for key in ['CreationTime', 'Expiration']:
            try:
                if key in self and isinstance(self[key], str):
                    self[key] = utils.parse_date(self[key])
            except ValueError:
                pass

    def __repr__(self):
        return "{}('{}')".format(type(self).__name__, self.hit_id)

    def __str__(self):
        return """{}:
    Production: {}
    Title: {}
    HIT Type: {}
    HIT Group: {}
    Status: {}
    Review Status: {}
    Reward: {}
    Pending: {}
    Available: {}
    Completed: {}""".format(self.hit_id, self.production, self.title, self.hit_type_id, self.hit_group_id,
                            self.status, self.review_status, self.reward, self.pending, self.available, self.completed)

    def refresh(self, get_assignments=False):
        self.update(mturk._get_hit(self.hit_id, self.__client)[0])
        if 'Assignments' in self or get_assignments:
            self.retrieve_assignments()

    def retrieve_assignments(self):
        self['Assignments'] = list(mturk.list_assignments_for_hit(self.hit_id))
        return self['Assignments']

    def retrieve_annotation(self):
        self['Annotation'] = mturk.parse_requester_annotation(self.get('RequesterAnnotation'))

    def add_assignments(self, additional_assignments):
        mturk.add_assignments(self.hit_id, additional_assignments)
        self['MaxAssignments'] += additional_assignments

    def __missing__(self, key):
        # TODO: Add support for accessing the properties as keys? hit['hit_id'] mapped to hit['HITId']
        if key == 'Assignments':
            self.retrieve_assignments()
            return self['Assignments']
        elif key == 'Annotation':
            self.retrieve_annotation()
            return self['Annotation']
        else:
            raise KeyError(key)

    @property
    def assignments(self):
        return self['Assignments']

    @property
    def answers(self):
        for assignment in self['Assignments']:
            answer = assignment['Answer'].copy()
            answer['WorkerId'] = assignment['WorkerId']
            yield answer

    @property
    def hit_id(self):
        return self['HITId']

    @property
    def reward(self):
        return float(self['Reward'])

    @property
    def reward_cents(self):
        return float(self['Reward']) * 100

    @property
    def hit_type_id(self):
        return self['HITTypeId']

    @property
    def hit_group_id(self):
        return self['HITGroupId']

    @property
    def hit_layout_id(self):
        return self.get('HITLayoutId', None)

    @property
    def creation_time(self):
        return self['CreationTime']

    @property
    def title(self):
        return self['Title']

    @property
    def description(self):
        return self['Description']

    @property
    def question(self):
        return self.get('Question', None)

    @property
    def keywords(self):
        return self['Keywords']

    @property
    def status(self):
        return self['HITStatus'] #: 'Assignable' | 'Unassignable' | 'Reviewable' | 'Reviewing' | 'Disposed',

    @property
    def max_assignments(self):
        return self.get('MaxAssignments', None)

    @property
    def auto_approval_delay(self):
        return self.get('AutoApprovalDelayInSeconds', None)

    @property
    def expiration(self):
        return self.get('Expiration', None)

    @property
    def duration(self):
        return self.get('AssignmentDurationInSeconds', None)

    @property
    def annotation(self):
        return self.get('Annotation', None)

    @property
    def qualification_requirements(self):
        return self.get('QualificationRequirements', None)

    @property
    def review_status(self):
        return self.get('HITReviewStatus', None)

    @property
    def pending(self):
        return self.get('NumberOfAssignmentsPending', None)

    @property
    def available(self):
        return self.get('NumberOfAssignmentsAvailable', None)

    @property
    def completed(self):
        return self.get('NumberOfAssignmentsCompleted', None)

    @property
    def production(self):
        return self.get('Production', None)

    @property
    def preview(self):
        return mturk.preview_url(self.hit_type_id, self.production)

    @property
    def completed_assignment_count(self):
        if 'Assignments' in self:
            return len(self['Assignments'])
        else:
            return 0

    def display_task_link(self):
        mturk.display_task_link(self.hit_type_id, self.production)

    def expire(self):
        mturk.expire_hit(self.hit_id, self.__client)
