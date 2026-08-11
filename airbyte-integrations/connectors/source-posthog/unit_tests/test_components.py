#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import os
from unittest.mock import MagicMock

import pytest as pytest
from source_posthog.components import EventsCartesianProductStreamSlicer

from airbyte_cdk.sources.declarative.datetime.min_max_datetime import MinMaxDatetime
from airbyte_cdk.sources.declarative.incremental.datetime_based_cursor import DatetimeBasedCursor
from airbyte_cdk.sources.declarative.partition_routers.list_partition_router import ListPartitionRouter
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.cursor_pagination_strategy import CursorPaginationStrategy
from airbyte_cdk.sources.declarative.requesters.request_option import RequestOption

# The connector directory (parent of this unit_tests/ dir). SourcePosthog resolves its
# "manifest.yaml" path relative to cwd (source.py: path_to_yaml="manifest.yaml"), so building
# the real source requires chdir'ing here first -- same as production's main.py/run.py entrypoint.
CONNECTOR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

POSTHOG_CONFIG = {
    "api_key": "test-key",
    "start_date": "2021-01-01T00:00:00+0000",
    "project_id": "2331",
}


stream_slicers = [
    ListPartitionRouter(values=[2331], cursor_field="project_id", config={}, parameters={}),
    DatetimeBasedCursor(
        start_datetime=MinMaxDatetime(datetime="2021-01-01T00:00:00.00+0000", datetime_format="%Y-%m-%dT%H:%M:%S.%f%z", parameters={}),
        end_datetime=MinMaxDatetime(datetime="2021-02-01T00:00:00.00+0000", datetime_format="%Y-%m-%dT%H:%M:%S.%f%z", parameters={}),
        step="P10D",
        cursor_field="timestamp",
        datetime_format="%Y-%m-%dT%H:%M:%S.%f%z",
        cursor_granularity="PT0.000001S",
        start_time_option=RequestOption(inject_into="request_parameter", field_name="after", parameters={}),
        end_time_option=RequestOption(inject_into="request_parameter", field_name="before", parameters={}),
        config={},
        parameters={},
    ),
]


@pytest.mark.parametrize(
    "test_name, initial_state, stream_slice, last_record, expected_state",
    [
        ("test_empty", {}, {}, {}, {}),
        (
            "test_set_initial_state",
            {"2331": {"timestamp": "2021-01-01T00:00:00.00+0000"}},
            {},
            {},
            {"2331": {"timestamp": "2021-01-01T00:00:00.00+0000"}},
        ),
        (
            "test_update_empty_state",
            {},
            {"project_id": "2331", "start_time": "2021-01-01T00:00:00.00+0000", "end_time": "2021-01-03T00:00:00.00+0000"},
            {"timestamp": "2021-01-01T11:00:00.00+0000"},
            {"2331": {"timestamp": "2021-01-01T11:00:00.00+0000"}},
        ),
        (
            "test_update_of_initial_state",
            {"2331": {"timestamp": "2021-01-01T10:00:00.00+0000"}},
            {"project_id": "2331", "start_time": "2021-01-01T00:00:00.00+0000", "end_time": "2021-01-03T00:00:00.00+0000"},
            {"timestamp": "2021-01-01T11:00:00.00+0000"},
            {"2331": {"timestamp": "2021-01-01T11:00:00.00+0000"}},
        ),
        (
            "test_update_of_initial_state_newly",
            {"2331": {"timestamp": "2021-01-01T22:00:00.00+0000"}},
            {"project_id": "2331", "start_time": "2021-01-01T00:00:00.00+0000", "end_time": "2021-01-03T00:00:00.00+0000"},
            {"timestamp": "2021-01-01T11:00:00.00+0000"},
            {"2331": {"timestamp": "2021-01-01T22:00:00.00+0000"}},
        ),
        (
            "test_update_of_initial_state_old_style",
            {"timestamp": "2021-01-01T10:00:00.00+0000"},
            {"project_id": "2331", "start_time": "2021-01-01T00:00:00.00+0000", "end_time": "2021-01-03T00:00:00.00+0000"},
            {"timestamp": "2021-01-01T11:00:00.00+0000"},
            {"2331": {"timestamp": "2021-01-01T11:00:00.00+0000"}, "timestamp": "2021-01-01T10:00:00.00+0000"},
        ),
    ],
)
def test_update_cursor(test_name, initial_state, stream_slice, last_record, expected_state):
    slicer = EventsCartesianProductStreamSlicer(stream_slicers=stream_slicers, parameters={})
    # set initial state
    slicer.set_initial_state(initial_state)

    if last_record:
        slicer.close_slice(stream_slice, last_record)

    updated_state = slicer.get_stream_state()
    assert updated_state == expected_state


@pytest.mark.parametrize(
    "test_name, stream_state, expected_stream_slices",
    [
        (
            "test_empty_state",
            {},
            [
                {"end_time": "2021-01-10T23:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-01T00:00:00.000000+0000"},
                {"end_time": "2021-01-20T23:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-10T23:59:59.999999+0000"},
                {"end_time": "2021-01-30T23:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-20T23:59:59.999999+0000"},
                {"end_time": "2021-02-01T00:00:00.000000+0000", "project_id": "2331", "start_time": "2021-01-30T23:59:59.999999+0000"},
            ],
        ),
        (
            "test_state",
            {"2331": {"timestamp": "2021-01-01T17:00:00.000000+0000"}},
            [
                {"end_time": "2021-01-11T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-01T17:00:00.000000+0000"},
                {"end_time": "2021-01-21T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-11T16:59:59.999999+0000"},
                {"end_time": "2021-01-31T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-21T16:59:59.999999+0000"},
                {"end_time": "2021-02-01T00:00:00.000000+0000", "project_id": "2331", "start_time": "2021-01-31T16:59:59.999999+0000"},
            ],
        ),
        (
            "test_old_stype_state",
            {"timestamp": "2021-01-01T17:00:00.000000+0000"},
            [
                {"end_time": "2021-01-11T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-01T17:00:00.000000+0000"},
                {"end_time": "2021-01-21T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-11T16:59:59.999999+0000"},
                {"end_time": "2021-01-31T16:59:59.999999+0000", "project_id": "2331", "start_time": "2021-01-21T16:59:59.999999+0000"},
                {"end_time": "2021-02-01T00:00:00.000000+0000", "project_id": "2331", "start_time": "2021-01-31T16:59:59.999999+0000"},
            ],
        ),
        (
            "test_state_for_one_slice",
            {"2331": {"timestamp": "2021-01-27T17:00:00.000000+0000"}},
            [{"end_time": "2021-02-01T00:00:00.000000+0000", "project_id": "2331", "start_time": "2021-01-27T17:00:00.000000+0000"}],
        ),
    ],
)
def test_stream_slices(test_name, stream_state, expected_stream_slices):
    slicer = EventsCartesianProductStreamSlicer(stream_slicers=stream_slicers, parameters={})
    slicer.set_initial_state(stream_state)
    stream_slices = slicer.stream_slices()
    assert list(stream_slices) == expected_stream_slices


# ---------------------------------------------------------------------------
# #2155 regression tests: EventsSimpleRetriever._request_params + manifest wiring
# ---------------------------------------------------------------------------
#
# Background: DatetimeBasedCursor.get_request_params() always re-injects the *stream
# slice's* after/before on every request regardless of next_page_token. For the events
# stream, PostHog's pagination is cursor-based (the response's 'next' url already carries
# its own narrower after/before). Re-injecting the slice's after/before on top of that
# duplicated & clobbered the paginator's params and reset pagination back to page 1
# forever -- an infinite loop. The fix is EventsSimpleRetriever._request_params(), wired
# in via manifest.yaml's CustomRetriever entry for the events stream.


@pytest.fixture
def events_retriever():
    """Builds the real SourcePosthog from the connector's actual manifest.yaml +
    components.py (not a hand-rolled substitute) and returns the events stream's
    retriever, so these tests exercise the real manifest wiring end to end, not just
    the EventsSimpleRetriever class in isolation.
    """
    original_cwd = os.getcwd()
    os.chdir(CONNECTOR_DIR)
    try:
        from source_posthog import SourcePosthog

        source = SourcePosthog()
        streams_by_name = {s.name: s for s in source.streams(POSTHOG_CONFIG)}
        retriever = streams_by_name["events"].retriever
        # Force construction of anything else manifest-path-dependent (e.g. slice
        # computation) while still chdir'ed, before we hand the retriever back out.
        list(retriever.stream_slices())
    finally:
        os.chdir(original_cwd)
    return retriever


def test_events_retriever_is_custom_class_not_silently_stock_simple_retriever(events_retriever):
    """Guards manifest.yaml's events_stream.retriever wiring (type: CustomRetriever,
    class_name: source_posthog.components.EventsSimpleRetriever).

    If a future manifest edit silently reverts this to a stock `type: SimpleRetriever`
    (or the class_name typo's out / the class fails to import and the factory falls back),
    the #2155 infinite-pagination-loop dead code returns to production with NO error at
    connector-build time -- the stream would just silently loop forever again. This test
    fails loudly on that regression by asserting the concrete class and cursor wiring.
    """
    assert type(events_retriever).__name__ == "EventsSimpleRetriever"
    # __post_init__'s manual cursor wiring (the CustomRetriever factory path doesn't
    # replicate create_simple_retriever()'s `cursor = stream_slicer if isinstance(...)`).
    assert events_retriever.cursor is not None
    assert events_retriever.cursor is events_retriever.stream_slicer
    # Confirms _request_params is actually overridden on this instance's class, not
    # merely inherited unchanged from SimpleRetriever.
    assert "_request_params" in type(events_retriever).__dict__


def test_request_params_without_next_page_token_includes_slice_after_before(events_retriever):
    """Page 1 (no next_page_token yet): the slice's real after/before must still flow
    through untouched -- the fix must only suppress them once pagination has started.
    """
    slices = list(events_retriever.stream_slices())
    assert len(slices) >= 1
    slice0 = slices[0]

    expected = events_retriever.stream_slicer.get_request_params(stream_slice=slice0, next_page_token=None)
    assert expected.get("after") is not None
    assert expected.get("before") is not None

    params = events_retriever._request_params(stream_state={}, stream_slice=slice0, next_page_token=None)
    assert params.get("after") == expected["after"]
    assert params.get("before") == expected["before"]


def test_request_params_with_next_page_token_suppresses_slice_after_before(events_retriever):
    """Page 2+ (paginating): the slice's real after/before must NOT leak into the
    request params -- this is the core of the #2155 fix. Without it, DatetimeBasedCursor
    re-injects the slice's own after/before over the paginator's narrower cursor-based
    before, clobbering it and resetting pagination back to page 1 forever.
    """
    slices = list(events_retriever.stream_slices())
    slice0 = slices[0]

    real_slice_params = events_retriever.stream_slicer.get_request_params(stream_slice=slice0, next_page_token=None)
    real_after, real_before = real_slice_params["after"], real_slice_params["before"]

    fake_next_page_token = {"next_page_token": "https://app.posthog.com/api/projects/2331/events/?after=X&before=Y"}
    params = events_retriever._request_params(
        stream_state={}, stream_slice=slice0, next_page_token=fake_next_page_token
    )

    assert params.get("after") != real_after
    assert params.get("before") != real_before
    # The fix suppresses the slice entirely (stream_slice -> {}), so no after/before
    # is re-derived from it at all.
    assert params.get("after") is None
    assert params.get("before") is None


def test_page_2_request_url_has_exactly_one_before_param(events_retriever):
    """THE regression test for the #2155 infinite pagination loop.

    Simulates receiving a real PostHog 'next' page URL and building the actual page-2
    HTTP request from it end to end (paginator.path() + requester._create_prepared_request()),
    exactly as the CDK's retriever loop does in production. Before the fix, the slice's
    own after/before params were re-injected on top of the ones already embedded in the
    'next' URL, producing a request URL with a duplicated/conflicting 'before' param whose
    *first* occurrence (the stale, wide one) won on the server side -- so every "next" page
    request effectively re-requested the same wide window forever, looping infinitely.
    """
    slices = list(events_retriever.stream_slices())
    slice0 = slices[0]

    fake_next_url = (
        "https://app.posthog.com/api/projects/2331/events/"
        "?after=2021-01-01T00%3A00%3A00.000000Z&before=2021-01-15T12%3A00%3A00.000000Z&limit=10000"
    )
    events_retriever.paginator._token = fake_next_url
    path = events_retriever._paginator_path()

    request_params = events_retriever._request_params(
        stream_state={}, stream_slice=slice0, next_page_token={"next_page_token": fake_next_url}
    )
    prepared = events_retriever.requester._create_prepared_request(path=path, params=request_params)

    assert prepared.url.count("before=") == 1
    assert prepared.url == fake_next_url


def test_cursor_pagination_strategy_stops_when_response_next_is_null():
    """Pagination-stop sanity check: when PostHog's response has no 'next' url left,
    CursorPaginationStrategy.next_page_token() must return None so the retriever's drive
    loop terminates. (Not itself part of the #2155 fix, but the other half of "does
    pagination behave" -- a fix that suppressed after/before but broke the stop condition
    would trade one infinite loop for another.)
    """
    strategy = CursorPaginationStrategy(
        cursor_value="{{ response['next'] }}",
        config={},
        parameters={},
        page_size=10000,
    )

    exhausted_response = MagicMock()
    exhausted_response.json.return_value = {"next": None, "results": []}
    exhausted_response.headers = {}
    exhausted_response.links = {}

    assert strategy.next_page_token(exhausted_response, []) is None


def test_cursor_pagination_strategy_returns_next_url_when_present():
    """Companion to the stop-condition test above: when 'next' IS present, it must be
    returned verbatim as the next page token so the retriever keeps paginating.
    """
    strategy = CursorPaginationStrategy(
        cursor_value="{{ response['next'] }}",
        config={},
        parameters={},
        page_size=10000,
    )

    next_url = "https://app.posthog.com/api/projects/2331/events/?after=X&before=Y&limit=10000"
    response_with_next = MagicMock()
    response_with_next.json.return_value = {"next": next_url, "results": [{"id": 1}]}
    response_with_next.headers = {}
    response_with_next.links = {}

    assert strategy.next_page_token(response_with_next, []) == next_url
