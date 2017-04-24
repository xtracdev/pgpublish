@events2pub
Feature: Events to Publish

  Scenario:
    Given events for aggregates in the publish table
    When I query for the events
    Then Events2Pub returns the events

  @snspub
  Scenario:
    Given a persisted aggregate
    When I query for events to publish
    Then I obtain all aggs with unpublished events
    And I can publish all events without error