version: 1

# Derived from Doris table `dbo_invoiceevent_state`, then hand-tuned.
# Column widths and scales below are checked against sample.sql.

context:
  reset: row

batch:
  rows: 1000

fields:
  # A counter zero-padded to 36 characters: same width as the UUID this used
  # to be, so it still fits varchar(256) and still costs 36 bytes per row.
  # A counter rather than `type: uuid` because dense strings compress, and the
  # three id columns here are the difference between 358 MiB and 139 MiB of
  # Parquet per 4M rows. Switch back to `type: uuid` when the point of the run
  # is random, incompressible data.
  #
  # The literal prefix carries 16 of the 36 characters, so only 20 digits are
  # zero-padded per value. That is worth about 5% on the whole pipeline over
  # padding all 36, and 20 digits is past the u64 counter range, so it cannot
  # overflow. A UUID-shaped template would leave only the last group, twelve
  # digits, and 40 TB of this data is about 1.1e12 rows -- past that ceiling.
  # The prefix also keeps the three id columns from being copies of each other.
  - name: invoiceid
    order: 0
    gen:
      type: sequence_string
      template: "inv0000000000000{}"
      width: 36

  # Partition key: AUTO PARTITION BY RANGE(date_trunc(eventdate, 'week')).
  # A 30-day window produces about five weekly partitions.
  - name: eventdate
    order: 1
    gen:
      type: datetime_around
      offset_seconds_min: -2592000
      offset_seconds_max: 0
      format: "%Y-%m-%d %H:%M:%S%.6f"

  # Distribution key: DISTRIBUTED BY HASH(organisationid) BUCKETS 8.
  # A fresh value per row spreads buckets perfectly but means every row is a
  # different organisation. See the notes on tenant skew below.
  - name: organisationid
    order: 2
    gen:
      type: sequence_string
      template: "org0000000000000{}"
      width: 36

  - name: eventid
    order: 3
    gen:
      type: sequence_string
      template: "evt0000000000000{}"
      width: 36

  # CDC operation. Both values are exactly 6 characters, which is the full
  # width of varchar(6), so no other verb fits without widening the column.
  - name: xc_cdc_operation
    order: 4
    gen:
      type: weighted_choice
      values:
        - value: INSERT
          weight: 80
        - value: UPDATE
          weight: 20

  # Declared as the merge-on-write sequence column
  # (function_column.sequence_col). Whichever row has the largest value here
  # wins a key collision.
  - name: xc_start_date_utc
    order: 5
    gen:
      type: datetime_around
      offset_seconds_min: -2592000
      offset_seconds_max: 0
      format: "%Y-%m-%d %H:%M:%S%.6f"

  - name: xc_shard_id
    order: 6
    gen:
      type: int_range
      min: 1
      max: 32

  # Load time, drawn from a recent window so it lands after the business event.
  - name: xc_etl_date_utc
    order: 7
    gen:
      type: datetime_around
      offset_seconds_min: -21600
      offset_seconds_max: 0
      format: "%Y-%m-%d %H:%M:%S%.6f"

  - name: sequence
    order: 8
    gen:
      type: int_range
      min: 1
      max: 1000000000

  # Invoice amounts as money: two decimal places between 1.00 and 99.99.
  # The column is decimal(19,4), so Doris stores these as 45.1200.
  - name: amount
    order: 9
    gen:
      type: decimal_range
      min: "1.00"
      max: "99.99"
      scale: 2

  # Invoice lifecycle events, weighted so creation and payment dominate and
  # exceptions stay rare.
  - name: eventtypecode
    order: 10
    gen:
      type: weighted_choice
      values:
        - value: INVOICE_CREATED
          weight: 20
        - value: INVOICE_ISSUED
          weight: 18
        - value: INVOICE_SENT
          weight: 15
        - value: PAYMENT_RECEIVED
          weight: 14
        - value: INVOICE_PAID
          weight: 10
        - value: REMINDER_SENT
          weight: 8
        - value: INVOICE_OVERDUE
          weight: 6
        - value: PAYMENT_FAILED
          weight: 4
        - value: CREDIT_NOTE_ISSUED
          weight: 2
        - value: INVOICE_VOIDED
          weight: 2
        - value: INVOICE_DISPUTED
          weight: 1

  # Outcome of the event above. Most events succeed.
  - name: eventstatuscode
    order: 11
    gen:
      type: weighted_choice
      values:
        - value: COMPLETED
          weight: 75
        - value: PENDING
          weight: 15
        - value: FAILED
          weight: 6
        - value: CANCELLED
          weight: 3
        - value: RETRYING
          weight: 1
