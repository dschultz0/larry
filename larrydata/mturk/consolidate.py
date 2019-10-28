

def _extract_response_detail(assignments, identifier, exclude_rejected=True):
    responses = []
    response_count = 0
    work_time = 0
    for assignment in assignments:
        responses.append({
            'WorkerId': assignment['WorkerId'],
            'Response': assignment['Answer'].get(identifier),
            'AssignmentId': assignment['AssignmentId'],
            'HITId': assignment['HITId'],
            'AcceptTime': assignment['AcceptTime'],
            'WorkTime': assignment['WorkTime'],
            'Excluded': assignment['AssignmentStatus'] == 'Rejected'
        })
        if exclude_rejected is False or assignment['AssignmentStatus'] != 'Rejected':
            response_count += 1
            work_time += int(assignment['WorkTime'].total_seconds())
    return {
        'Responses': responses,
        'ResponseCount': response_count,
        'WorkTime': work_time,
        'Identifier': identifier
    }


def _score_text_responses(response_detail):
    scores = {}
    for response in response_detail['Responses']:
        value = response['Response']
        score = scores.get(value, 0)
        scores[value] = score + 1
    return scores


def _consolidate_text_response(assignments, identifier, threshold, exclude_rejected=True):
    response_detail = _extract_response_detail(assignments, identifier, exclude_rejected=exclude_rejected)
    response_detail['ScoredResponses'] = _score_text_responses(response_detail)
    answer = None
    for response, score in response_detail['ScoredResponses'].items():
        if score * 100 / response_detail['ResponseCount'] >= threshold:
            answer = response
            break
    if answer is not None:
        for response in response_detail['Responses']:
            response['Accuracy'] = response['Response'] == answer
    return answer, response_detail


def consolidate_crowd_classifier(assignments, threshold=60, exclude_rejected=True):
    """
    Retrieves Worker responses for a HITId and computes a consolidated answer based on a simple plurality of responses.
    For example, if the HIT has 3 Assignments, and Workers respond with responses of A, A, and B, the resulting
    response would be A since 66.7% of Workers agree on a response of A which is higher than the default threshold of
    60%. If the threshold were set at 80% than None would be returned. Similarly, if Workers responded with A, B, and C,
    the result would be None since none of the answers received 60% of responses. By default, Assignments that have
    already been rejected are ignored for purposes of scoring responses.
    :param assignments: A list or generator containing the assignments to process
    :param threshold: A 0-100 percentage value (80 = 80%) to use a a threshold in looking for agreement amongst Workers
    :param exclude_rejected: Boolean value (default=True) indicating that Assignments that have already been
    rejected should be excluded
    :return: A tuple containing the result and an object with detail on the responses for use in measuring Worker
    accuracy
    """
    return _consolidate_text_response(
        assignments,
        'category.label',
        threshold,
        exclude_rejected=exclude_rejected)