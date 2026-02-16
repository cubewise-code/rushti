import { useState } from 'react'
import { GitBranch, Sparkles, Shield, Database, Lock, Cpu, RefreshCw, ChevronLeft, ChevronRight } from 'lucide-react'
import { useScrollAnimation } from '../hooks/useScrollAnimation'

const DOCS_BASE = 'https://cubewise-code.github.io/rushti/docs'

const features = [
  {
    icon: GitBranch,
    title: 'DAG Execution',
    description: 'Intelligent parallel execution with true dependency awareness. Tasks run as soon as their dependencies complete, maximizing throughput.',
    bgColor: 'bg-cyan-50',
    iconColor: 'text-cyan-600',
    borderColor: 'border-cyan-200',
    link: `${DOCS_BASE}/features/dag-execution/`,
  },
  {
    icon: Sparkles,
    title: 'Self-Optimization',
    description: 'EWMA-based learning with configurable scheduling algorithms. Choose shortest-first or longest-first to match your workload.',
    bgColor: 'bg-orange-50',
    iconColor: 'text-orange-600',
    borderColor: 'border-orange-200',
    link: `${DOCS_BASE}/features/optimization/`,
  },
  {
    icon: Database,
    title: 'SQLite Database',
    description: 'Persistent storage for execution history and performance statistics. Query past runs, analyze trends, and optimize workflows.',
    bgColor: 'bg-blue-50',
    iconColor: 'text-blue-600',
    borderColor: 'border-blue-200',
    link: `${DOCS_BASE}/features/statistics/`,
  },
  {
    icon: Lock,
    title: 'Exclusive Mode',
    description: 'Prevents resource conflicts and data corruption by ensuring only one RushTI workflow executes at a time in shared TM1 environments.',
    bgColor: 'bg-purple-50',
    iconColor: 'text-purple-600',
    borderColor: 'border-purple-200',
    link: `${DOCS_BASE}/features/exclusive-mode/`,
  },
  {
    icon: Shield,
    title: 'Checkpoint & Resume',
    description: 'Never lose progress. Automatic checkpoints enable resuming from the exact point of failure, reducing recovery time by 95%.',
    bgColor: 'bg-emerald-50',
    iconColor: 'text-emerald-600',
    borderColor: 'border-emerald-200',
    link: `${DOCS_BASE}/features/checkpoint-resume/`,
  },
  {
    icon: Cpu,
    title: 'TM1 Integration',
    description: 'Native integration with IBM TM1 and Planning Analytics. Direct execution of TI processes with full monitoring and logging support.',
    bgColor: 'bg-sky-50',
    iconColor: 'text-sky-600',
    borderColor: 'border-sky-200',
    link: `${DOCS_BASE}/features/tm1-integration/`,
  },
  {
    icon: RefreshCw,
    title: '100% Backwards Compatible',
    description: 'Existing task files work out of the box, no changes required. When you\'re ready, optimize them to unlock DAG scheduling and all new features.',
    bgColor: 'bg-amber-50',
    iconColor: 'text-amber-600',
    borderColor: 'border-amber-200',
    link: `${DOCS_BASE}/advanced/migration-guide/`,
  },
]

export default function Features() {
  const [ref, isVisible] = useScrollAnimation()
  const [currentSlide, setCurrentSlide] = useState(0)

  const nextSlide = () => {
    setCurrentSlide((prev) => (prev + 1) % features.length)
  }

  const prevSlide = () => {
    setCurrentSlide((prev) => (prev - 1 + features.length) % features.length)
  }

  const goToSlide = (index) => {
    setCurrentSlide(index)
  }

  return (
    <section id="features" className="py-24 relative bg-slate-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16" ref={ref}>
          <h2 className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-4 animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            Powerful Features
          </h2>
          <p className={`text-slate-600 text-lg max-w-2xl mx-auto animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '100ms' }}>
            Everything you need to transform sequential TI execution into intelligent, parallel workflows.
          </p>
        </div>

        {/* Carousel Container */}
        <div className={`relative animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '200ms' }}>
          {/* Navigation Buttons */}
          <button
            onClick={prevSlide}
            className="absolute left-4 sm:left-8 top-1/2 -translate-y-1/2 z-20 bg-white rounded-full p-3 shadow-lg hover:shadow-xl transition-all hover:scale-110 border border-slate-200"
            aria-label="Previous feature"
          >
            <ChevronLeft className="w-6 h-6 text-slate-700" />
          </button>

          <button
            onClick={nextSlide}
            className="absolute right-4 sm:right-8 top-1/2 -translate-y-1/2 z-20 bg-white rounded-full p-3 shadow-lg hover:shadow-xl transition-all hover:scale-110 border border-slate-200"
            aria-label="Next feature"
          >
            <ChevronRight className="w-6 h-6 text-slate-700" />
          </button>

          {/* Slides Container with perspective */}
          <div className="relative h-[450px] sm:h-[400px]">
            <div className="absolute inset-0 flex items-center justify-center">
              {features.map((feature, index) => {
                let offset = index - currentSlide

                // Wrap around for circular effect
                if (offset > features.length / 2) {
                  offset -= features.length
                } else if (offset < -features.length / 2) {
                  offset += features.length
                }

                const isCenter = offset === 0
                const isAdjacent = Math.abs(offset) === 1

                return (
                  <div
                    key={feature.title}
                    className="absolute w-full transition-all duration-500 ease-in-out"
                    style={{
                      transform: `translateX(${offset * 60}%) scale(${isCenter ? 1 : 0.85})`,
                      opacity: isCenter ? 1 : isAdjacent ? 0.4 : 0,
                      zIndex: isCenter ? 10 : isAdjacent ? 5 : 0,
                      pointerEvents: isCenter ? 'auto' : 'none',
                      filter: isCenter ? 'blur(0px)' : 'blur(3px)',
                    }}
                  >
                    <div className="px-4 sm:px-8">
                      <div
                        className={`${feature.bgColor} ${feature.borderColor} relative overflow-hidden border-2 rounded-3xl p-6 sm:p-8 max-w-xl mx-auto shadow-xl ${isCenter ? 'hover:shadow-2xl' : ''} transition-all`}
                        style={{
                          background: `linear-gradient(135deg, ${feature.bgColor === 'bg-cyan-50' ? '#ecfeff 0%, #cffafe 100%' :
                            feature.bgColor === 'bg-orange-50' ? '#fff7ed 0%, #ffedd5 100%' :
                            feature.bgColor === 'bg-blue-50' ? '#eff6ff 0%, #dbeafe 100%' :
                            feature.bgColor === 'bg-purple-50' ? '#faf5ff 0%, #f3e8ff 100%' :
                            feature.bgColor === 'bg-emerald-50' ? '#ecfdf5 0%, #d1fae5 100%' :
                            '#f0f9ff 0%, #e0f2fe 100%'})`,
                        }}
                      >
                        {/* Glossy overlay */}
                        <div className="absolute inset-0 bg-gradient-to-br from-white/40 via-transparent to-transparent pointer-events-none" />
                        <div className="absolute top-0 right-0 w-48 h-48 bg-white/20 rounded-full blur-3xl pointer-events-none" />

                        <div className="relative flex flex-col h-full justify-between">
                          <div className="flex flex-col sm:flex-row gap-4 sm:gap-6">
                            {/* Left side - Icon and Title */}
                            <div className="flex flex-col items-center sm:items-start gap-3 sm:w-[35%]">
                              <div className={`w-16 h-16 sm:w-20 sm:h-20 rounded-2xl bg-white/90 backdrop-blur-sm flex items-center justify-center shadow-lg border border-white/50`}>
                                <feature.icon className={`w-8 h-8 sm:w-10 sm:h-10 ${feature.iconColor}`} />
                              </div>
                              <h3 className="text-xl sm:text-2xl font-bold text-slate-900 text-center sm:text-left leading-tight">
                                {feature.title}
                              </h3>
                            </div>

                            {/* Right side - Description */}
                            <div className="flex-1 flex items-center py-2">
                              <p className="text-slate-700 text-base sm:text-lg leading-relaxed text-center sm:text-left">
                                {feature.description}
                              </p>
                            </div>
                          </div>

                          {/* Learn more link */}
                          <div className="flex justify-center sm:justify-end mt-4">
                            <a
                              href={feature.link}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center gap-2 text-sm font-semibold text-[#0085CA] hover:text-[#00AEEF] transition-colors group"
                            >
                              Learn more
                              <svg className="w-4 h-4 transition-transform group-hover:translate-x-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                              </svg>
                            </a>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>

          {/* Dots Navigation */}
          <div className="flex justify-center gap-2 mt-8">
            {features.map((_, index) => (
              <button
                key={index}
                onClick={() => goToSlide(index)}
                className={`transition-all rounded-full ${
                  currentSlide === index
                    ? 'w-8 h-3 bg-gradient-to-r from-[#0085CA] to-[#00AEEF]'
                    : 'w-3 h-3 bg-slate-300 hover:bg-slate-400'
                }`}
                aria-label={`Go to slide ${index + 1}`}
              />
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
