import { Calendar, RefreshCw, Clock } from 'lucide-react'
import { useScrollAnimation } from '../hooks/useScrollAnimation'

const useCases = [
  {
    icon: Calendar,
    title: 'Monthly Financial Close',
    description: 'Coordinate complex month-end processes with multiple data sources, calculations, and reports. RushTI ensures the right order while maximizing parallelism.',
    gradient: 'from-sky-500 to-cyan-500',
  },
  {
    icon: RefreshCw,
    title: 'Daily Data Refresh',
    description: 'Load data from multiple ERPs, data warehouses, and external sources simultaneously. Failed loads automatically retry without blocking other processes.',
    gradient: 'from-amber-500 to-orange-500',
  },
  {
    icon: Clock,
    title: 'Overnight Batch Processing',
    description: 'Run comprehensive calculations, allocations, and cube rebuilds overnight. Checkpoint ensures morning teams start with completed data, not crashed jobs.',
    gradient: 'from-emerald-500 to-teal-500',
  },
]

export default function UseCases() {
  const [ref, isVisible] = useScrollAnimation()

  return (
    <section id="use-cases" className="py-24 relative bg-slate-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16" ref={ref}>
          <h2 className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-4 animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            Built for Real Workflows
          </h2>
          <p className={`text-slate-600 text-lg max-w-2xl mx-auto animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '100ms' }}>
            RushTI powers mission-critical IBM PA processes at organizations worldwide.
          </p>
        </div>

        {/* Use case cards */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {useCases.map((useCase, index) => (
            <div
              key={useCase.title}
              className={`feature-card p-8 animate-on-scroll ${isVisible ? 'visible' : ''}`}
              style={{ transitionDelay: `${(index + 2) * 100}ms` }}
            >
              <div className={`w-14 h-14 rounded-2xl bg-gradient-to-br ${useCase.gradient} flex items-center justify-center mb-6`}>
                <useCase.icon className="w-7 h-7 text-white" />
              </div>
              <h3 className="text-2xl font-semibold text-slate-900 mb-3">{useCase.title}</h3>
              <p className="text-slate-600 leading-relaxed">{useCase.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
