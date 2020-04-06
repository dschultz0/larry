import uuid
import warnings
from html import escape


def display_iframe(url=None, html=None, width=None, height=600, frame_border=5):
    frame_id = 'f' + str(uuid.uuid4())[:8]
    w = 900 if width is None else width
    try:
        from IPython.display import display, HTML
        if html:
            _html = '<iframe id="{}" width="{}" height="{}" srcdoc="{}" frameborder="{}" allowfullscreen></iframe>'\
                    .format(frame_id, w, height, escape(html), frame_border)
        else:
            _html = '<iframe id="{}" width="{}" height="{}" src="{}" frameborder="{}" allowfullscreen></iframe>'\
                    .format(frame_id, w, height, url, frame_border)

        if width is None:
            _html += '''<script language="JavaScript">
                document.getElementById("{0}").width = document.getElementById("{0}").parentElement.clientWidth - 25
                window.addEventListener("resize", function(){{
                    document.getElementById("{0}").width = document.getElementById("{0}").parentElement.clientWidth - 25
                }})</script>'''.format(frame_id)

        # this will throw an irrelevant warning about considering the iframe element
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            display(HTML(_html))

    except ImportError as e:
        raise Exception('Display operations are not supported outside of the IPython environment')


def display_html(html):
    try:
        from IPython.display import display, HTML
        display(HTML(html))
    except ImportError as e:
        raise Exception('Display operations are not supported outside of the IPython environment')


def display_link(url, prefix):
    display_html('<p><strong>{0}:</strong> <a href="{1}" target="_blank">{1}</a></p>'.format(prefix, url))
