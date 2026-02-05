create schema if not exists bronze;
-- ================
-- Product events
-- ================
create table if not exists bronze.product_events (
  event_id         text primary key,
  event_ts         timestamptz not null,
  ingested_ts      timestamptz not null default now(),
  event_name       text not null,
  event_version    int not null,
  source_system    text not null,
  environment      text not null,

  customer_id      text not null,
  user_id          text not null,
  session_id       text null,

  feature_name     text null,
  feature_action   text null,

  channel          text null,
  device           text null,
  country          text null,

  raw_payload      jsonb null
);

-- ================
-- Billing/subscription events
-- ================
create table if not exists bronze.billing_events (
  event_id         text primary key,
  event_ts         timestamptz not null,
  ingested_ts      timestamptz not null default now(),
  event_name       text not null,
  event_version    int not null,
  source_system    text not null,
  environment      text not null,

  customer_id      text not null,
  user_id          text null,

  subscription_id  text null,
  invoice_id       text null,

  plan_id          text null,
  old_plan_id      text null,
  new_plan_id      text null,

  amount_usd       numeric(12,2) null,

  raw_payload      jsonb null
);

-- ================
-- Payment events (gateway)
-- ================
create table if not exists bronze.payment_events (
  event_id         text primary key,
  event_ts         timestamptz not null,
  ingested_ts      timestamptz not null default now(),
  event_name       text not null,
  event_version    int not null,
  source_system    text not null,
  environment      text not null,

  customer_id      text not null,
  user_id          text null,

  invoice_id       text not null,
  payment_id       text not null,
  attempt_number   int not null,

  amount_usd       numeric(12,2) not null,
  status           text null,
  failure_reason   text null,

  raw_payload      jsonb null
);

-- Helpful indexes for validation & incremental loads
create index if not exists idx_product_events_event_ts on bronze.product_events(event_ts);
create index if not exists idx_billing_events_event_ts on bronze.billing_events(event_ts);
create index if not exists idx_payment_events_event_ts on bronze.payment_events(event_ts);
create index if not exists idx_payment_events_invoice_id on bronze.payment_events(invoice_id);
