@sns
Feature: SNS

  Scenario: SNS Topic Check
    Given a valid topic ARN
    When check it
    Then I receive no errors

  Scenario: Invalid SBS Topic Check
    Given an invalid top ARN
    When I check it
    Then I receive an error