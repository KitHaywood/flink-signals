-- Seed lookup tables with baseline data.

INSERT INTO strategies (strategy_id, name, description)
VALUES (
    uuid_generate_v4(),
    'sma_cross',
    'Simple moving average crossover baseline strategy'
)
ON CONFLICT (name) DO NOTHING;
