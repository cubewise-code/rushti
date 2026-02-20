import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { Database, History, Search, AlertTriangle } from 'lucide-react'

export default function SqliteShowcase() {
  const [ref, isVisible] = useScrollAnimation()

  return (
    <section className="py-32 relative overflow-hidden bg-white">
      {/* Background gradient orbs - subtle */}
      <div className="absolute w-[800px] h-[800px] rounded-full bg-cyan-400/5 blur-3xl -right-40 top-1/2 -translate-y-1/2" />
      <div className="absolute w-[600px] h-[600px] rounded-full bg-sky-400/5 blur-3xl -left-20 bottom-0" />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center" ref={ref}>
          {/* Left side - Text content */}
          <div className={`animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            <div className="inline-flex items-center space-x-2 bg-sky-50 border border-sky-200 rounded-full px-4 py-2 mb-6">
              <Database className="w-4 h-4 text-sky-600" />
              <span className="text-sky-700 text-sm font-medium">SQLite Analytics</span>
            </div>

            <h2 className="text-4xl sm:text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
              Track your
              <br />
              <span className="bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent">workflow trends</span>
            </h2>

            <p className="text-slate-600 text-lg mb-8 leading-relaxed max-w-lg">
              Every execution is recorded in a local SQLite database. Identify which workflows are slowing down,
              spot anomalies, and make data-driven decisions to keep your TM1 environment running smoothly.
            </p>

            {/* Stats cards */}
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-slate-50 border border-slate-200 rounded-xl p-4">
                <History className="w-5 h-5 text-sky-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">30+</div>
                <div className="text-slate-500 text-xs">Days History</div>
              </div>
              <div className="bg-slate-50 border border-slate-200 rounded-xl p-4">
                <Search className="w-5 h-5 text-emerald-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">Zero</div>
                <div className="text-slate-500 text-xs">Config Required</div>
              </div>
              <div className="bg-slate-50 border border-slate-200 rounded-xl p-4">
                <AlertTriangle className="w-5 h-5 text-amber-500 mb-2" />
                <div className="text-2xl font-bold text-slate-900">Early</div>
                <div className="text-slate-500 text-xs">Issue Detection</div>
              </div>
            </div>
          </div>

          {/* Right side - SQL IDE mockup with 3D tilt - LIGHT THEME */}
          <div
            className={`relative animate-on-scroll ${isVisible ? 'visible' : ''}`}
            style={{
              transitionDelay: '200ms',
              perspective: '1000px'
            }}
          >
            {/* IDE Container with 3D transform */}
            <div
              className="relative"
              style={{
                transform: 'rotateY(-8deg) rotateX(5deg)',
                transformStyle: 'preserve-3d'
              }}
            >
              {/* Glow effect behind */}
              <div className="absolute -inset-4 bg-gradient-to-r from-sky-400/10 to-cyan-400/10 rounded-2xl blur-2xl" />

              {/* IDE Window - Light Theme */}
              <div className="relative bg-white border border-slate-200 rounded-xl overflow-hidden shadow-2xl">
                {/* Window header */}
                <div className="flex items-center justify-between px-4 py-3 bg-slate-50 border-b border-slate-200">
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 rounded-full bg-red-400" />
                    <div className="w-3 h-3 rounded-full bg-amber-400" />
                    <div className="w-3 h-3 rounded-full bg-emerald-400" />
                  </div>
                  <div className="flex items-center space-x-2">
                    <Database className="w-4 h-4 text-slate-400" />
                    <span className="text-slate-500 text-sm font-mono">rushti_stats.db</span>
                  </div>
                  <div className="w-16" /> {/* Spacer for centering */}
                </div>

                {/* SQL Query section */}
                <div className="p-4 border-b border-slate-100">
                  <div className="flex items-center space-x-2 mb-3">
                    <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">Query</span>
                    <div className="flex-1 h-px bg-slate-100" />
                  </div>
                  <pre className="text-sm font-mono overflow-x-auto leading-relaxed">
                    <code>
                      <span className="text-slate-400">-- Find workflows slowing down over last 10 runs</span>
                      {'\n'}
                      <span className="text-purple-600 font-medium">SELECT</span>
                      {'\n'}
                      <span className="text-slate-700">    workflow,</span>
                      {'\n'}
                      <span className="text-slate-700">    </span>
                      <span className="text-sky-600">ROUND</span>
                      <span className="text-slate-700">(</span>
                      <span className="text-sky-600">AVG</span>
                      <span className="text-slate-700">(duration_s), 1) </span>
                      <span className="text-purple-600 font-medium">as</span>
                      <span className="text-emerald-600"> avg_time</span>
                      <span className="text-slate-700">,</span>
                      {'\n'}
                      <span className="text-slate-700">    </span>
                      <span className="text-sky-600">ROUND</span>
                      <span className="text-slate-700">(</span>
                      <span className="text-sky-600">MAX</span>
                      <span className="text-slate-700">(duration_s) - </span>
                      <span className="text-sky-600">MIN</span>
                      <span className="text-slate-700">(duration_s), 1) </span>
                      <span className="text-purple-600 font-medium">as</span>
                      <span className="text-emerald-600"> variance</span>
                      {'\n'}
                      <span className="text-purple-600 font-medium">FROM</span>
                      <span className="text-amber-600"> runs </span>
                      <span className="text-purple-600 font-medium">WHERE</span>
                      <span className="text-slate-700"> run_date {'>'} </span>
                      <span className="text-sky-600">DATE</span>
                      <span className="text-slate-700">(</span>
                      <span className="text-amber-600">'now'</span>
                      <span className="text-slate-700">, </span>
                      <span className="text-amber-600">'-10 days'</span>
                      <span className="text-slate-700">)</span>
                      {'\n'}
                      <span className="text-purple-600 font-medium">GROUP BY</span>
                      <span className="text-slate-700"> workflow </span>
                      <span className="text-purple-600 font-medium">ORDER BY</span>
                      <span className="text-slate-700"> variance </span>
                      <span className="text-purple-600 font-medium">DESC</span>
                      <span className="text-slate-700">;</span>
                    </code>
                  </pre>
                </div>

                {/* Results table */}
                <div className="p-4 bg-slate-50/50">
                  <div className="flex items-center space-x-2 mb-3">
                    <span className="text-xs text-emerald-600 uppercase tracking-wider font-medium">Results</span>
                    <span className="text-xs text-slate-400">(4 rows)</span>
                    <div className="flex-1 h-px bg-slate-200" />
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm font-mono">
                      <thead>
                        <tr className="text-slate-500 text-left">
                          <th className="pb-2 pr-4 font-medium">workflow</th>
                          <th className="pb-2 pr-4 font-medium text-right">avg_time</th>
                          <th className="pb-2 font-medium text-right">variance</th>
                        </tr>
                      </thead>
                      <tbody className="text-slate-700">
                        <tr className="border-t border-slate-200 bg-red-50">
                          <td className="py-2 pr-4 text-slate-700">daily_finance_close</td>
                          <td className="py-2 pr-4 text-right">892.4s</td>
                          <td className="py-2 text-right text-red-600 font-medium">+247.3s ⚠</td>
                        </tr>
                        <tr className="border-t border-slate-200 bg-amber-50">
                          <td className="py-2 pr-4 text-slate-700">cube_refresh_all</td>
                          <td className="py-2 pr-4 text-right">445.2s</td>
                          <td className="py-2 text-right text-amber-600 font-medium">+89.1s</td>
                        </tr>
                        <tr className="border-t border-slate-200">
                          <td className="py-2 pr-4 text-slate-700">export_reports</td>
                          <td className="py-2 pr-4 text-right">234.8s</td>
                          <td className="py-2 text-right text-slate-500">+12.4s</td>
                        </tr>
                        <tr className="border-t border-slate-200">
                          <td className="py-2 pr-4 text-slate-700">nightly_backup</td>
                          <td className="py-2 pr-4 text-right">156.3s</td>
                          <td className="py-2 text-right text-emerald-600 font-medium">-3.2s ✓</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              {/* Floating badge */}
              <div
                className="absolute -bottom-4 -left-4 bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-lg"
                style={{ transform: 'translateZ(40px)' }}
              >
                <div className="flex items-center space-x-2">
                  <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
                  <span className="text-xs text-slate-600">Portable, no database setup needed</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
