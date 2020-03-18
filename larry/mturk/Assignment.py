from larry import utils
from larry import mturk
import collections


class Assignment(collections.UserDict):
    def __init__(self, data, mturk_client=None):
        self.__client = mturk_client
        collections.UserDict.__init__(self)
        if isinstance(data, str):
            assignment, hit = mturk._get_assignment(data, mturk_client)
            self.update(assignment)
        else:
            self.update(data)
            self._parse_datetime_values()
        if isinstance(self.get('Answer'), str):
            self['Answer'] = mturk.parse_answers(self['Answer'])
        if 'SubmitTime' in self and 'AcceptTime' in self:
            self['WorkTime'] = round((self['SubmitTime'] - self['AcceptTime']).total_seconds())

    def _parse_datetime_values(self):
        for key in ['AutoApprovalTime', 'AcceptTime', 'SubmitTime', 'ApprovalTime', 'RejectionTime', 'Deadline']:
            try:
                if key in self and isinstance(self[key], str):
                    self[key] = utils.parse_date(self[key])
            except ValueError:
                pass

    def __repr__(self):
        return "{}('{}')".format(type(self).__name__, self.assignment_id)

    def __str__(self):
        return """{}:
   Status: {}
   Worker: {}
   HIT: {}
   Accept Time: {}
   Work Time: {}
   Answer: {}""".format(self.assignment_id, self.status, self.worker_id, self.hit_id, self.accept_time,
                       self.work_time, self.answer)

    def refresh(self):
        self.update(mturk._get_assignment(self.assignment_id, self.__client)[0])

    @property
    def assignment_id(self):
        return self['AssignmentId']

    @property
    def worker_id(self):
        return self['WorkerId']

    @property
    def hit_id(self):
        return self['HITId']

    @property
    def status(self):
        return self['AssignmentStatus'] # 'Submitted' | 'Approved' | 'Rejected'

    @property
    def auto_approval_time(self):
        return self.get('AutoApprovalTime', None)

    @property
    def accept_time(self):
        return self.get('AcceptTime', None)

    @property
    def submit_time(self):
        return self.get('SubmitTime', None)

    @property
    def approval_time(self):
        return self.get('ApprovalTime', None)

    @property
    def rejection_time(self):
        return self.get('RejectionTime', None)

    @property
    def work_time(self):
        return self.get('WorkTime', None)

    @property
    def deadline(self):
        return self.get('Deadline', None)

    @property
    def answer(self):
        return self.get('Answer', None)

    @property
    def requester_feedback(self):
        return self.get('RequesterFeedback', None)
