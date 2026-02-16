import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { Sparkles, TrendingUp, Clock, Zap } from 'lucide-react'

export default function OptimizationShowcase() {
  const [ref, isVisible] = useScrollAnimation()

  // Simulated run data showing improvement over time
  const runData = [
    { run: 1, time: 699, label: 'Initial' },
    { run: 2, time: 672, label: '' },
    { run: 3, time: 651, label: '' },
    { run: 4, time: 618, label: '' },
    { run: 5, time: 598, label: '' },
    { run: 6, time: 571, label: '' },
    { run: 7, time: 559, label: '' },
    { run: 8, time: 552, label: '' },
    { run: 9, time: 547, label: '' },
    { run: 10, time: 544, label: 'Optimized' },
  ]

  const maxTime = 720
  const minTime = 500

  return (
    <section className="py-32 relative overflow-hidden bg-slate-50">
      {/* Background gradient orbs - subtle */}
      <div className="absolute w-[800px] h-[800px] rounded-full bg-orange-400/5 blur-3xl -left-40 top-1/2 -translate-y-1/2" />
      <div className="absolute w-[600px] h-[600px] rounded-full bg-amber-400/5 blur-3xl -right-20 bottom-0" />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center" ref={ref}>
          {/* Left side - Learning curve visualization */}
          <div
            className={`relative animate-on-scroll ${isVisible ? 'visible' : ''}`}
            style={{
              transitionDelay: '200ms',
              perspective: '1000px'
            }}
          >
            {/* Chart Container with 3D transform */}
            <div
              className="relative"
              style={{
                transform: 'rotateY(8deg) rotateX(5deg)',
                transformStyle: 'preserve-3d'
              }}
            >
              {/* Glow effect behind */}
              <div className="absolute -inset-4 bg-gradient-to-r from-orange-400/10 to-amber-400/10 rounded-2xl blur-2xl" />

              {/* Chart Window */}
              <div className="relative bg-white border border-slate-200 rounded-xl overflow-hidden shadow-2xl">
                {/* Window header */}
                <div className="flex items-center justify-between px-4 py-3 bg-slate-50 border-b border-slate-200">
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 rounded-full bg-red-400" />
                    <div className="w-3 h-3 rounded-full bg-amber-400" />
                    <div className="w-3 h-3 rounded-full bg-emerald-400" />
                  </div>
                  <div className="flex items-center space-x-2">
                    <Sparkles className="w-4 h-4 text-amber-500" />
                    <span className="text-slate-500 text-sm font-medium">EWMA Optimization</span>
                  </div>
                  <div className="w-16" />
                </div>

                {/* Chart content */}
                <div className="p-6">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <div className="text-sm text-slate-500 mb-1">daily_finance_close</div>
                      <div className="text-xs text-slate-400">Last 10 optimized runs</div>
                    </div>
                    <div className="text-right">
                      <div className="text-2xl font-bold text-emerald-600">-22.2%</div>
                      <div className="text-xs text-slate-400">improvement</div>
                    </div>
                  </div>

                  {/* Bar chart */}
                  <div className="space-y-2">
                    {runData.map((data, i) => {
                      const width = ((data.time - minTime) / (maxTime - minTime)) * 100
                      const isFirst = i === 0
                      const isLast = i === runData.length - 1

                      return (
                        <div key={data.run} className="flex items-center gap-3">
                          <div className="w-8 text-xs text-slate-400 text-right">#{data.run}</div>
                          <div className="flex-1 h-6 bg-slate-100 rounded overflow-hidden relative">
                            <div
                              className={`h-full rounded transition-all duration-1000 ${
                                isFirst ? 'bg-gradient-to-r from-red-400 to-red-500' :
                                isLast ? 'bg-gradient-to-r from-emerald-400 to-emerald-500' :
                                'bg-gradient-to-r from-amber-400 to-orange-400'
                              }`}
                              style={{
                                width: isVisible ? `${width}%` : '0%',
                                transitionDelay: `${i * 100 + 300}ms`
                              }}
                            />
                            {/* Time label inside bar */}
                            <div className="absolute inset-0 flex items-center px-2">
                              <span className={`text-xs font-medium ${width > 30 ? 'text-white' : 'text-slate-600'}`}>
                                {data.time}s
                              </span>
                            </div>
                          </div>
                          {data.label && (
                            <div className={`text-xs font-medium w-16 ${isLast ? 'text-emerald-600' : 'text-red-500'}`}>
                              {data.label}
                            </div>
                          )}
                          {!data.label && <div className="w-16" />}
                        </div>
                      )
                    })}
                  </div>

                  {/* Legend */}
                  <div className="flex items-center justify-center gap-6 mt-6 pt-4 border-t border-slate-100">
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 rounded bg-gradient-to-r from-red-400 to-red-500" />
                      <span className="text-xs text-slate-500">Before optimization</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 rounded bg-gradient-to-r from-emerald-400 to-emerald-500" />
                      <span className="text-xs text-slate-500">After EWMA learning</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Floating badge */}
              <div
                className="absolute -bottom-4 -right-4 bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-lg"
                style={{ transform: 'translateZ(40px)' }}
              >
                <div className="flex items-center space-x-2">
                  <Sparkles className="w-4 h-4 text-amber-500" />
                  <span className="text-xs text-slate-600">155s saved per run</span>
                </div>
              </div>
            </div>
          </div>

          {/* Right side - Text content */}
          <div className={`animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            <div className="inline-flex items-center space-x-2 bg-amber-50 border border-amber-200 rounded-full px-4 py-2 mb-6">
              <Sparkles className="w-4 h-4 text-amber-600" />
              <span className="text-amber-700 text-sm font-medium">Self-Optimization</span>
            </div>

            <h2 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
              Gets smarter
              <br />
              <span className="bg-gradient-to-r from-amber-600 to-orange-500 bg-clip-text text-transparent">with every run</span>
            </h2>

            <p className="text-slate-600 text-lg mb-8 leading-relaxed max-w-lg">
              RushTI's EWMA-based learning algorithm analyzes your execution history and continuously
              refines task scheduling. The more you run, the better it gets.
            </p>

            {/* Stats cards */}
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
                <TrendingUp className="w-5 h-5 text-emerald-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">22%</div>
                <div className="text-slate-500 text-xs">Faster Runs</div>
              </div>
              <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
                <Clock className="w-5 h-5 text-sky-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">155s</div>
                <div className="text-slate-500 text-xs">Time Saved</div>
              </div>
              <div className="bg-white border border-slate-200 rounded-xl p-4 shadow-sm">
                <Zap className="w-5 h-5 text-amber-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">Better</div>
                <div className="text-slate-500 text-xs">CPU Utilization</div>
              </div>
            </div>

            {/* How it works */}
            <div className="mt-8 p-4 bg-gradient-to-r from-amber-50 to-orange-50 rounded-xl border border-amber-100">
              <div className="text-sm font-medium text-slate-700 mb-2">How EWMA optimization works:</div>
              <ul className="text-sm text-slate-600 space-y-1">
                <li className="flex items-start gap-2">
                  <span className="text-amber-500 mt-0.5">•</span>
                  Tracks actual execution times vs estimates
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-amber-500 mt-0.5">•</span>
                  Weighs recent runs more heavily for accuracy
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-amber-500 mt-0.5">•</span>
                  Choose <code className="text-amber-700 bg-amber-50 px-1 rounded">shortest_first</code> or <code className="text-amber-700 bg-amber-50 px-1 rounded">longest_first</code> scheduling
                </li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
