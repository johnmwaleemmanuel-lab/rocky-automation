'use client';

/**
 * Config-driven Rise/Fall trade controls.
 *
 * Renders the SAME functional controls as TradeControls, but the Rise/Fall,
 * Duration and Stake controls each have 3 style variants, and the rows render
 * in a configurable order. Fully functional (uses the real trading handlers).
 * Theme colour comes from the app's --primary (existing branding pipeline), so
 * `bg-primary` / `text-primary` pick it up automatically.
 *
 * Used by the editor (/edit), preview (/preview) and the deployed app when a
 * RiseFallAppConfig is present. The original TradeControls is untouched.
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { GripVertical, TrendingUp } from 'lucide-react';
import { toast } from 'sonner';
import { useRearrangeDrag } from '@/hooks/use-rearrange-drag';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import { cn } from '@/lib/utils';
import { EndTimePicker } from '@/components/custom/end-time-picker';
import type { DerivWS, ActiveSymbol, ProposalInfo, BuyResult } from '@deriv/core';
import type { Direction, DurationSelectUnit, DurationOption } from '../lib/types';
import type { BlockKey, ControlKey, RiseFallAppConfig, StyleVariant } from '../lib/app-config';

/** Human labels shown on each draggable block in rearrange mode. */
const BLOCK_LABELS: Record<BlockKey, string> = {
  chart: 'Chart + symbol',
  riseFall: 'Rise / Fall',
  allowEquals: 'Allow equals',
  stake: 'Stake',
  duration: 'Duration',
  buy: 'Buy button',
};

export interface ConfigurableTradeControlsProps {
  config: RiseFallAppConfig;
  direction: Direction;
  onDirectionChange: (direction: Direction) => void;
  allowEquals: boolean;
  onAllowEqualsChange: (value: boolean) => void;
  isConnected: boolean;
  stake: string;
  onStakeChange: (value: string) => void;
  duration: number;
  onDurationChange: (value: number) => void;
  durationOptions: DurationOption[];
  durationUnit: DurationSelectUnit;
  onDurationUnitChange: (unit: DurationSelectUnit) => void;
  endDate: Date | undefined;
  onEndDateChange: (date: Date | undefined) => void;
  endTime: string;
  onEndTimeChange: (time: string) => void;
  ws: DerivWS | null;
  activeSymbol: ActiveSymbol | null;
  proposal: ProposalInfo | null;
  onBuy: () => void;
  isBuying: boolean;
  buyResult: BuyResult | null;
  buyError: string | null;
  onClearBuyResult: () => void;
  isAuthenticated?: boolean;
  /** Edit mode — control rows become selectable (click opens its accordion). */
  editMode?: boolean;
  /** Called when a control row is clicked in edit mode. */
  onSelect?: (key: ControlKey) => void;
  /** The currently selected control (highlighted). */
  selectedKey?: string | null;
  /**
   * Rearrange mode — blocks become draggable to reorder the layout directly in
   * the phone (chart + symbol move as one). Component editing is disabled while
   * on. Only meaningful together with `editMode`.
   */
  rearrangeMode?: boolean;
  /** Called with the new block order after a drag-drop reorder. */
  onReorder?: (order: BlockKey[]) => void;
  /**
   * The chart + symbol-dropdown block, rendered at the `chart` position in the
   * order. It manages its own edit selection, so it's placed as-is (not wrapped
   * in a selectable row).
   */
  chartSlot?: React.ReactNode;
}

export function ConfigurableTradeControls(props: ConfigurableTradeControlsProps) {
  const {
    config,
    direction,
    onDirectionChange,
    allowEquals,
    onAllowEqualsChange,
    isConnected,
    stake,
    onStakeChange,
    duration,
    onDurationChange,
    durationOptions,
    durationUnit,
    onDurationUnitChange,
    endDate,
    onEndDateChange,
    endTime,
    onEndTimeChange,
    ws,
    activeSymbol,
    proposal,
    onBuy,
    isBuying,
    buyResult,
    buyError,
    onClearBuyResult,
    isAuthenticated,
    editMode,
    onSelect,
    selectedKey,
    rearrangeMode,
    onReorder,
    chartSlot,
  } = props;

  const rearrange = useRearrangeDrag<BlockKey>(config.order, (next) => onReorder?.(next));

  // Flash the draggable blocks once — the first time the layout is unlocked in
  // this session — so the user notices the components can be dragged.
  const [hasFlashed, setHasFlashed] = useState(false);
  useEffect(() => {
    if (!rearrangeMode || hasFlashed) return;
    const timer = window.setTimeout(() => setHasFlashed(true), 2000);
    return () => window.clearTimeout(timer);
  }, [rearrangeMode, hasFlashed]);

  // Scroll the selected control into view in edit mode, so opening a component's
  // accordion always reveals it in the phone (lower rows aren't left off-screen).
  const rowRefs = useRef<Record<string, HTMLElement | null>>({});
  useEffect(() => {
    if (!editMode || !selectedKey) return;
    const el = rowRefs.current[selectedKey];
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }, [editMode, selectedKey]);

  useEffect(() => {
    if (buyError) {
      toast.error('Purchase Failed', { description: buyError });
      onClearBuyResult();
    }
  }, [buyError, onClearBuyResult]);
  useEffect(() => {
    if (buyResult) {
      toast.success('Contract Purchased', {
        description: `Buy price: ${buyResult.buyPrice.toFixed(2)} USD | Payout: ${buyResult.payout.toFixed(2)} USD | Balance: ${buyResult.balanceAfter.toFixed(2)} USD`,
      });
      onClearBuyResult();
    }
  }, [buyResult, onClearBuyResult]);

  const activeOption = durationOptions.find((option) => option.unit === durationUnit);
  const endTimeOption = durationOptions.find((option) => option.unit === 'end-time');
  const { endTimeMinDate, endTimeMaxDate } = useMemo(() => {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return {
      endTimeMinDate: today,
      endTimeMaxDate: endTimeOption
        ? new Date(today.getTime() + endTimeOption.max * 86400000)
        : new Date(today.getTime() + 365 * 86400000),
    };
  }, [endTimeOption]);

  // ── Rise / Fall (3 styles) ──────────────────────────────────────────────
  const renderRiseFall = () => {
    const isRise = direction === 'CALL';
    const isFall = direction === 'PUT';

    const variants: Record<StyleVariant, () => React.ReactNode> = {
      // a — segmented toggle (default)
      a: () => (
        <ToggleGroup
          type="single"
          value={direction}
          onValueChange={(value) => { if (value === 'CALL' || value === 'PUT') onDirectionChange(value); }}
          className="w-full gap-0 rounded-full bg-muted p-1"
        >
          <ToggleGroupItem value="CALL" className="flex-1 rounded-full text-sm font-medium text-muted-foreground data-[state=on]:bg-background data-[state=on]:text-emerald-600 data-[state=on]:font-bold data-[state=on]:shadow-sm hover:text-foreground">
            Rise
          </ToggleGroupItem>
          <ToggleGroupItem value="PUT" className="flex-1 rounded-full text-sm font-medium text-muted-foreground data-[state=on]:bg-background data-[state=on]:text-rose-600 data-[state=on]:font-bold data-[state=on]:shadow-sm hover:text-foreground">
            Fall
          </ToggleGroupItem>
        </ToggleGroup>
      ),
      // Side-by-side solid buttons — the active direction is filled (Rise green,
      // Fall red), the other stays muted.
      b: () => (
        <div className="flex gap-2">
          <Button
            size="lg"
            variant="secondary"
            className={cn('flex-1 rounded-xl font-semibold', isRise && 'bg-emerald-500 text-white hover:bg-emerald-600')}
            onClick={() => onDirectionChange('CALL')}
          >
            Rise ↑
          </Button>
          <Button
            size="lg"
            variant="secondary"
            className={cn('flex-1 rounded-xl font-semibold', isFall && 'bg-rose-500 text-white hover:bg-rose-600')}
            onClick={() => onDirectionChange('PUT')}
          >
            Fall ↓
          </Button>
        </div>
      ),
      // Isolated chips — outline when unselected, filled colour when selected.
      c: () => (
        <div className="flex gap-3">
          <Button
            size="lg"
            variant="outline"
            className={cn(
              'flex-1 rounded-full font-semibold',
              isRise && 'border-emerald-500 bg-emerald-500 text-white hover:bg-emerald-600 hover:text-white'
            )}
            onClick={() => onDirectionChange('CALL')}
          >
            Rise ↑
          </Button>
          <Button
            size="lg"
            variant="outline"
            className={cn(
              'flex-1 rounded-full font-semibold',
              isFall && 'border-rose-500 bg-rose-500 text-white hover:bg-rose-600 hover:text-white'
            )}
            onClick={() => onDirectionChange('PUT')}
          >
            Fall ↓
          </Button>
        </div>
      ),
    };
    return (variants[config.styles.riseFall] ?? variants.a)();
  };

  // ── Allow equals (3 styles) ─────────────────────────────────────────────
  const renderAllowEquals = () => {
    const variants: Record<StyleVariant, () => React.ReactNode> = {
      // a — switch (default)
      a: () => (
        <div className="flex items-center justify-between">
          <Label htmlFor="allow-equals" className="text-sm cursor-pointer">Allow equals</Label>
          <Switch id="allow-equals" checked={allowEquals} onCheckedChange={onAllowEqualsChange} />
        </div>
      ),
      // Checkbox + label
      b: () => (
        <div className="flex items-center gap-2.5">
          <Checkbox
            id="allow-equals"
            checked={allowEquals}
            onCheckedChange={(checked) => onAllowEqualsChange(checked === true)}
          />
          <Label htmlFor="allow-equals" className="text-sm cursor-pointer">Allow equals</Label>
        </div>
      ),
      // Segmented Off / On
      c: () => (
        <div className="flex items-center justify-between gap-3">
          <Label className="text-sm">Allow equals</Label>
          <ToggleGroup
            type="single"
            value={allowEquals ? 'on' : 'off'}
            onValueChange={(val) => { if (val) onAllowEqualsChange(val === 'on'); }}
            className="gap-0 rounded-full bg-muted p-1"
          >
            <ToggleGroupItem value="off" className="rounded-full px-4 text-xs text-muted-foreground data-[state=on]:bg-background data-[state=on]:font-semibold data-[state=on]:shadow-sm">
              Off
            </ToggleGroupItem>
            <ToggleGroupItem value="on" className="rounded-full px-4 text-xs text-muted-foreground data-[state=on]:bg-background data-[state=on]:text-primary data-[state=on]:font-semibold data-[state=on]:shadow-sm">
              On
            </ToggleGroupItem>
          </ToggleGroup>
        </div>
      ),
    };
    return (variants[config.styles.allowEquals] ?? variants.a)();
  };

  // ── Stake (3 styles) ────────────────────────────────────────────────────
  const renderStake = () => {
    const setStakeNum = (amount: number) => onStakeChange(String(Math.max(0, amount)));
    const current = parseFloat(stake) || 0;
    const variants: Record<StyleVariant, () => React.ReactNode> = {
      // a — plain input (original)
      a: () => (
        <div className="space-y-1.5">
          <Label htmlFor="stake" className="text-xs text-muted-foreground">Stake</Label>
          <Input
            id="stake"
            type="number"
            value={stake}
            onChange={(event) => onStakeChange(event.target.value)}
            onKeyDown={(event) => { if (['e', 'E', '+', '-'].includes(event.key)) event.preventDefault(); }}
            min={0}
            step="0.01"
            labelRight="USD"
          />
        </div>
      ),
      // input with −/+ steppers
      b: () => (
        <div className="space-y-1.5">
          <Label className="text-xs text-muted-foreground">Stake</Label>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="icon" onClick={() => setStakeNum(current - 1)}>−</Button>
            <Input type="number" value={stake} onChange={(event) => onStakeChange(event.target.value)} min={0} step="0.01" labelRight="USD" className="text-center" />
            <Button variant="outline" size="icon" onClick={() => setStakeNum(current + 1)}>+</Button>
          </div>
        </div>
      ),
      // preset chips + input
      c: () => (
        <div className="space-y-1.5">
          <Label className="text-xs text-muted-foreground">Stake</Label>
          <div className="flex gap-2">
            {['5', '10', '25', '50'].map((preset) => (
              <Button key={preset} variant={stake === preset ? 'default' : 'outline'} size="sm" className={cn('flex-1', stake === preset && 'bg-primary text-primary-foreground')} onClick={() => onStakeChange(preset)}>
                {preset}
              </Button>
            ))}
          </div>
          <Input type="number" value={stake} onChange={(event) => onStakeChange(event.target.value)} min={0} step="0.01" labelRight="USD" />
        </div>
      ),
    };
    return (variants[config.styles.stake] ?? variants.a)();
  };

  // ── Duration (3 styles) ─────────────────────────────────────────────────
  const renderDurationValue = (variant: RiseFallAppConfig['styles']['duration']) => {
    if (durationUnit === 'end-time') {
      return (
        <EndTimePicker
          ws={ws}
          isConnected={isConnected}
          activeSymbol={activeSymbol}
          endDate={endDate}
          onEndDateChange={onEndDateChange}
          endTime={endTime}
          onEndTimeChange={onEndTimeChange}
          minDate={endTimeMinDate}
          maxDate={endTimeMaxDate}
        />
      );
    }
    if (variant === 'b') {
      // stepper
      const clamp = (amount: number) => Math.min(activeOption?.max ?? amount, Math.max(activeOption?.min ?? 1, amount));
      return (
        <div className="flex items-center justify-between rounded-lg border border-border p-1">
          <Button variant="ghost" size="icon" onClick={() => onDurationChange(clamp(duration - 1))}>−</Button>
          <span className="font-semibold">{duration}</span>
          <Button variant="ghost" size="icon" onClick={() => onDurationChange(clamp(duration + 1))}>+</Button>
        </div>
      );
    }
    return (
      <Input
        type="number"
        value={duration}
        onChange={(event) => { const val = parseInt(event.target.value, 10); if (!isNaN(val)) onDurationChange(val); }}
        min={activeOption?.min}
        max={activeOption?.max}
        step={1}
      />
    );
  };
  const renderDuration = () => {
    const variant = config.styles.duration;
    const unitControl =
      variant === 'c' ? (
        <ToggleGroup
          type="single"
          value={durationUnit}
          onValueChange={(value) => {
            const opt = durationOptions.find((option) => option.unit === value);
            if (opt) onDurationUnitChange(opt.unit);
          }}
          className="w-full gap-1 rounded-lg bg-muted p-1"
        >
          {durationOptions.map((opt) => (
            <ToggleGroupItem key={opt.unit} value={opt.unit} className="flex-1 rounded-md text-xs data-[state=on]:bg-background data-[state=on]:text-primary data-[state=on]:font-semibold">
              {opt.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      ) : (
        <Select
          value={durationUnit}
          onValueChange={(value) => {
            const opt = durationOptions.find((option) => option.unit === value);
            if (opt) onDurationUnitChange(opt.unit);
          }}
        >
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            {durationOptions.map((opt) => (
              <SelectItem key={opt.unit} value={opt.unit}>{opt.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      );
    return (
      <div className="space-y-1.5">
        <Label className="text-xs text-muted-foreground">Duration</Label>
        {unitControl}
        {renderDurationValue(variant)}
      </div>
    );
  };

  // ── Buy (3 styles, themed) ──────────────────────────────────────────────
  const renderBuy = () => {
    const disabled = !isConnected || !proposal || isBuying;
    const payout = proposal ? proposal.payout.toFixed(2) : null;

    const variants: Record<StyleVariant, () => React.ReactNode> = {
      // a — pill (default)
      a: () => (
        <Button
          className="w-full rounded-full bg-primary hover:bg-primary/90 text-primary-foreground"
          size="lg"
          disabled={disabled}
          onClick={onBuy}
        >
          {isBuying ? (
            'Purchasing...'
          ) : (
            <span className="flex flex-col items-center leading-tight gap-0.5">
              <span>Buy</span>
              {payout && <span className="text-xs font-normal opacity-90">Payout {payout} USD</span>}
            </span>
          )}
        </Button>
      ),
      // Block — squared, bold, payout shown as a badge on the right.
      b: () => (
        <Button
          className="w-full h-14 rounded-2xl bg-primary hover:bg-primary/90 text-primary-foreground text-base font-bold"
          disabled={disabled}
          onClick={onBuy}
        >
          <span className="flex w-full items-center justify-between px-1">
            <span>{isBuying ? 'Purchasing...' : 'Buy'}</span>
            {payout && (
              <span className="rounded-full bg-white/20 px-2.5 py-0.5 text-xs font-medium">
                {payout} USD
              </span>
            )}
          </span>
        </Button>
      ),
      // Gradient with an upward-trend icon + payout below.
      c: () => (
        <Button
          className="w-full h-14 rounded-xl bg-gradient-to-r from-primary to-primary/70 hover:opacity-90 text-primary-foreground shadow-lg shadow-primary/20"
          disabled={disabled}
          onClick={onBuy}
        >
          <span className="flex flex-col items-center leading-tight gap-0.5">
            <span className="flex items-center gap-1.5 font-semibold">
              <TrendingUp className="h-4 w-4" />
              {isBuying ? 'Purchasing...' : 'Buy'}
            </span>
            {payout && <span className="text-xs font-normal opacity-90">Payout {payout} USD</span>}
          </span>
        </Button>
      ),
    };
    return (variants[config.styles.buy] ?? variants.a)();
  };

  const renderers: Record<ControlKey, () => React.ReactNode> = {
    riseFall: renderRiseFall,
    allowEquals: renderAllowEquals,
    stake: renderStake,
    duration: renderDuration,
    buy: renderBuy,
  };

  if (editMode && rearrangeMode) {
    // Rearrange mode: every block (incl. the chart + symbol, which move as one)
    // is draggable to reorder the layout directly in the phone. Inner content is
    // inert (pointer-events-none) so dragging never triggers the controls.
    return (
      <div className="w-full space-y-2">
        {config.order.map((key) => {
          const isChart = key === 'chart';
          // Desktop layout: the chart lives in its own fixed left column (no
          // chartSlot here), so it isn't a reorderable block — skip it.
          if (isChart && !chartSlot) return null;
          const dragging = rearrange.draggingKey === key;
          const over = rearrange.overKey === key;
          return (
            <div
              key={key}
              {...rearrange.getItemProps(key)}
              className={cn(
                'group relative cursor-grab rounded-xl border-2 border-dashed bg-card/40 transition-all active:cursor-grabbing',
                !hasFlashed && 'nocode-drag-hint',
                'border-border',
                // Hover affordance (only when not mid-drag) — tint border + fill.
                !rearrange.isDragging && 'hover:border-primary/60 hover:bg-primary/5 hover:shadow-sm',
                // Drop target while dragging — stronger colour fill.
                over && 'border-primary bg-primary/10 ring-2 ring-primary/40',
                dragging && 'opacity-40',
              )}
            >
              {/* Drag-handle chip — also labels the block. Highlights on hover. */}
              <div
                className={cn(
                  'absolute left-2 top-2 z-[70] flex items-center gap-1 rounded-md bg-background/90 px-1.5 py-1 text-[11px] font-medium text-muted-foreground shadow-sm ring-1 ring-border transition-colors',
                  !rearrange.isDragging && 'group-hover:text-primary group-hover:ring-primary/40',
                  over && 'text-primary ring-primary/40',
                )}
              >
                <GripVertical className="h-3.5 w-3.5" />
                {BLOCK_LABELS[key]}
              </div>
              {/* Transparent overlay so a drag can start anywhere on the block —
                  essential over the chart canvas, which otherwise swallows it. */}
              <div className="absolute inset-0 z-[60]" />
              <div className="pointer-events-none select-none px-2 pb-2 pt-9">
                {isChart ? chartSlot : renderers[key as ControlKey]()}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  if (editMode) {
    // Each block is selectable: clicking opens its accordion in the dashboard.
    // The chart block manages its own selection, so it's placed as-is.
    return (
      <div className="w-full space-y-3">
        {config.order.map((key) => {
          if (key === 'chart') {
            return (
              <div
                key="chart"
                ref={(el) => {
                  rowRefs.current.chart = el;
                }}
                className="overflow-hidden rounded-xl border border-border bg-background shadow-sm"
              >
                {chartSlot}
              </div>
            );
          }
          const selected = selectedKey === key;
          return (
            <button
              key={key}
              type="button"
              ref={(el) => {
                rowRefs.current[key] = el;
              }}
              onClick={() => onSelect?.(key)}
              className={[
                'group relative block w-full rounded-xl border-2 bg-background p-3 text-left shadow-sm transition-colors',
                selected ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/60',
              ].join(' ')}
            >
              <div
                className={[
                  'pointer-events-none absolute inset-0 z-10 rounded-xl bg-primary/10 transition-opacity',
                  selected ? 'opacity-100' : 'opacity-0 group-hover:opacity-100',
                ].join(' ')}
              />
              <div className="pointer-events-none">{renderers[key]()}</div>
            </button>
          );
        })}
      </div>
    );
  }

  return (
    <div className="w-full space-y-3 lg:space-y-4">
      {config.order.map((key) => {
        // Chart only renders where a chartSlot is provided (the no-code mobile
        // column). On desktop the chart lives in its own column, so it's omitted
        // here to avoid rendering a second chart.
        if (key === 'chart') return chartSlot ? <div key="chart">{chartSlot}</div> : null;
        return <div key={key}>{renderers[key]()}</div>;
      })}
      {isAuthenticated && (
        <Button asChild variant="ghost" className="w-full text-sm text-muted-foreground hover:text-foreground">
          <Link href="/reports">View your positions →</Link>
        </Button>
      )}
    </div>
  );
}
