import { cn } from "@/lib/utils";

export function Panel({
  title,
  subtitle,
  action,
  className,
  bodyClassName,
  children,
  style,
}: {
  title?: string;
  subtitle?: string;
  action?: React.ReactNode;
  className?: string;
  bodyClassName?: string;
  children: React.ReactNode;
  style?: React.CSSProperties;
}) {
  return (
    <section
      style={style}
      className={cn(
        "rise group relative overflow-hidden rounded-xl border border-border bg-card shadow-sm",
        className,
      )}
    >
      {(title || action) && (
        <div className="flex items-start justify-between gap-4 px-5 pt-4">
          <div>
            {title && (
              <h2 className="text-sm font-semibold tracking-tight text-foreground">{title}</h2>
            )}
            {subtitle && <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p>}
          </div>
          {action && <div className="shrink-0">{action}</div>}
        </div>
      )}
      <div className={cn("px-2 pb-2 pt-3 sm:px-3", bodyClassName)}>{children}</div>
    </section>
  );
}
