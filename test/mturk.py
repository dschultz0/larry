import unittest
import larry as lry

ENVIRONMENT_PROD = 'production'
ENVIRONMENT_SANDBOX = 'sandbox'
SANDBOX_HIT = '39HYCOOPKNK26VOMWWPV050D1O9MD5'
SANDBOX_HIT_TYPE = '3W679PTMVMW4B1YPP05F1CL2SYKBXP'
SANDBOX_ASSIGNMENT = '3TEM0PF1Q5W8Q0F8XU7ZRSPG1ARD0O'
PROD_HIT = '30Y6N4AHYOVT3B1E15NSX07Z8YNRDS'
PROD_HIT_TYPE = '32CVJ4DS80UD0FXOVYK5MQJIWDSKV8'
PROD_ASSIGNMENT = '3N4BPTXIO8RWKSXYNI9LV8K4SNYUK5'
SIMPLE_QUESTION = '<script src="https://assets.crowd.aws/crowd-html-elements.js"></script><crowd-form><p>What is the date today?</p><input name="date"></crowd-form>'
SIMPLE_TEMPLATE = '<script src="https://assets.crowd.aws/crowd-html-elements.js"></script><crowd-form><p>What day of the week was {{ date }}?</p><input name="date"></crowd-form>'
SIMPLE_TEMPLATE_URI = 's3://larry-testing/test-objects/mturk/simple_template.html'
BASIC_ANNOTATION_DICT = {'path': 'detail'}
BASIC_ANNOTATION_STRING = 'For easier data science use Larry'

EXTERNAL_URL = 'https://www.google.com'


class MTurkTests(unittest.TestCase):

    def test_use_production(self):
        lry.mturk.use_production()
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_PROD)
        self.assertTrue(lry.mturk.production())
        self.assertFalse(lry.mturk.sandbox())

    def test_use_sandbox(self):
        lry.mturk.use_sandbox()
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_SANDBOX)
        self.assertTrue(lry.mturk.sandbox())
        self.assertFalse(lry.mturk.production())

    def test_set_environment_prod(self):
        lry.mturk.set_environment('prod')
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_PROD)
        self.assertTrue(lry.mturk.production())
        self.assertFalse(lry.mturk.sandbox())

    def test_set_environment_sandbox(self):
        lry.mturk.set_environment('sandbox')
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_SANDBOX)
        self.assertTrue(lry.mturk.sandbox())
        self.assertFalse(lry.mturk.production())

    def test_set_environment_prod_hit(self):
        lry.mturk.set_environment(hit_id=PROD_HIT)
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_PROD)
        self.assertTrue(lry.mturk.production())
        self.assertFalse(lry.mturk.sandbox())

    def test_set_environment_sandbox_hit(self):
        lry.mturk.set_environment(hit_id=SANDBOX_HIT)
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_SANDBOX)
        self.assertTrue(lry.mturk.sandbox())
        self.assertFalse(lry.mturk.production())

    def test_set_environment_prod_assignment(self):
        lry.mturk.set_environment(assignment_id=PROD_ASSIGNMENT)
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_PROD)
        self.assertTrue(lry.mturk.production())
        self.assertFalse(lry.mturk.sandbox())

    def test_set_environment_sandbox_assignment(self):
        lry.mturk.set_environment(assignment_id=SANDBOX_ASSIGNMENT)
        self.assertEqual(lry.mturk.environment(), ENVIRONMENT_SANDBOX)
        self.assertTrue(lry.mturk.sandbox())
        self.assertFalse(lry.mturk.production())

    def test_create_hit(self):
        lry.mturk.use_sandbox()
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward_cents=10, lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   html_question=SIMPLE_QUESTION, annotation=BASIC_ANNOTATION_DICT)
        self.assertFalse(hit.production)
        hit = lry.mturk.get_hit(hit.hit_id)
        self.assertEqual(hit.annotation, BASIC_ANNOTATION_DICT)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward_cents=10, lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   html_question=SIMPLE_QUESTION, annotation=BASIC_ANNOTATION_STRING)
        self.assertFalse(hit.production)
        hit = lry.mturk.get_hit(hit.hit_id)
        self.assertEqual(hit.annotation, BASIC_ANNOTATION_STRING)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward='0.10', lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   question=lry.mturk.render_html_question(SIMPLE_QUESTION))
        self.assertFalse(hit.production)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward='0.10', lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   question=lry.mturk.render_external_question(EXTERNAL_URL))
        self.assertFalse(hit.production)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward='0.10', lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   external_question=EXTERNAL_URL)
        self.assertFalse(hit.production)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward='0.10', lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   question_template=SIMPLE_TEMPLATE, template_context={'date': '2/13/2020'})
        self.assertFalse(hit.production)
        hit = lry.mturk.create_hit("Simple task", "Answer a simple question", reward='0.10', lifetime=60,
                                   assignment_duration=60, max_assignments=1, auto_approval_delay=600,
                                   question_template_uri=SIMPLE_TEMPLATE_URI, template_context={'date': '2/13/2020'})
        self.assertFalse(hit.production)

    def test_create_by_hit_type(self):
        lry.mturk.use_sandbox()
        hit_type_id = lry.mturk.create_hit_type(title="Simple task", description="Answer a simple question",
                                                reward="0.10", assignment_duration=60)
        hit_type_id = lry.mturk.create_hit_type(title="Simple task", description="Answer a simple question",
                                                reward_cents=10, assignment_duration=60, auto_approval_delay=60,
                                                keywords='foo,bar')
        hit = lry.mturk.create_hit(hit_type_id=hit_type_id, lifetime=60, max_assignments=1,
                                   html_question=SIMPLE_QUESTION)
        self.assertFalse(hit.production)


if __name__ == '__main__':
    unittest.main()
