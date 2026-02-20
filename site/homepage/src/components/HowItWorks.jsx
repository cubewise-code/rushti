import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { useState, useEffect, useRef } from 'react'
import { RotateCcw } from 'lucide-react'

// Timeline visualization comparing Level-based vs DAG execution
function ExecutionTimeline() {
  const [hasAnimated, setHasAnimated] = useState(false)
  const [progress, setProgress] = useState(0)
  const [isComplete, setIsComplete] = useState(false)
  const timelineRef = useRef(null)
  const intervalRef = useRef(null)

  // Tasks with their execution characteristics and levels for coloring
  // Level 1: No predecessors (blue shades)
  // Level 2: Depends on level 1 (green shades)
  // Level 3: Depends on level 2 (purple shades)
  const tasks = [
    { id: 1, name: "Load Exchange Rates", duration: 2, predecessor: null, level: 1 },
    { id: 2, name: "Load GL Data", duration: 2, predecessor: null, level: 1 },
    { id: 3, name: "Load Actuals", duration: 7, predecessor: null, level: 1 },
    { id: 4, name: "Load Budget", duration: 2, predecessor: 2, level: 2 },
    { id: 5, name: "Calculate Variance", duration: 5, predecessor: 2, level: 2 },
    { id: 6, name: "Build Reports", duration: 2, predecessor: 4, level: 3 },
  ]

  // Color schemes for each level
  const levelColors = {
    1: { from: 'from-sky-500', to: 'to-sky-400', text: 'text-white', bg: 'bg-sky-500' },
    2: { from: 'from-emerald-500', to: 'to-emerald-400', text: 'text-white', bg: 'bg-emerald-500' },
    3: { from: 'from-violet-500', to: 'to-violet-400', text: 'text-white', bg: 'bg-violet-500' },
  }

  // Level-based execution (RushTI 1.6): groups by levels, waits for entire level to complete
  const levelExecution = [
    { id: 1, start: 0, end: 2, level: 1 },
    { id: 2, start: 0, end: 2, level: 1 },
    { id: 3, start: 0, end: 7, level: 1 },
    { id: 4, start: 7, end: 9, level: 2 },
    { id: 5, start: 7, end: 12, level: 2 },
    { id: 6, start: 12, end: 14, level: 3 },
  ]

  // DAG-based execution (RushTI 2.0): starts as soon as dependencies are met
  const dagExecution = [
    { id: 1, start: 0, end: 2, level: 1 },
    { id: 2, start: 0, end: 2, level: 1 },
    { id: 3, start: 0, end: 7, level: 1 },
    { id: 4, start: 2, end: 4, level: 2 },
    { id: 5, start: 2, end: 7, level: 2 },
    { id: 6, start: 4, end: 6, level: 3 },
  ]

  const levelTotal = 14
  const dagTotal = 7
  const maxTime = 14
  const timeScale = 100 / maxTime

  const startAnimation = () => {
    setProgress(0)
    setIsComplete(false)
    setHasAnimated(true)

    let p = 0
    intervalRef.current = setInterval(() => {
      p += 2
      if (p >= 100) {
        p = 100
        clearInterval(intervalRef.current)
        setIsComplete(true)
      }
      setProgress(p)
    }, 50)
  }

  const handleReplay = () => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current)
    }
    startAnimation()
  }

  // Trigger animation when scrolled into view
  useEffect(() => {
    const currentRef = timelineRef.current
    let triggered = false

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !triggered) {
          triggered = true
          setTimeout(() => {
            startAnimation()
          }, 200)
        }
      },
      { threshold: 0.05 }
    )

    if (currentRef) {
      observer.observe(currentRef)
    }

    return () => {
      observer.disconnect()
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [])

  const getBarWidth = (start, end, currentProgress) => {
    const progressTime = (currentProgress / 100) * maxTime
    if (progressTime <= start) return 0
    if (progressTime >= end) return (end - start) * timeScale
    return (progressTime - start) * timeScale
  }

  const getBarOpacity = (start, currentProgress) => {
    const progressTime = (currentProgress / 100) * maxTime
    return progressTime >= start ? 1 : 0.3
  }

  return (
    <div ref={timelineRef} className="bg-white rounded-2xl p-8 shadow-xl shadow-slate-200/50 border border-slate-100">
      {/* Timeline header */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">Timeline (seconds)</span>
          <button
            onClick={handleReplay}
            className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-sky-600 transition-colors"
            title="Replay animation"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            <span>Replay</span>
          </button>
        </div>
        {/* Time markers */}
        <div className="flex justify-between text-xs text-slate-400 mb-1 ml-32">
          {[0, 2, 4, 6, 8, 10, 12, 14].map(t => (
            <span key={t} style={{ width: '12.5%', textAlign: t === 0 ? 'left' : 'center' }}>{t}s</span>
          ))}
        </div>
        <div className="h-px bg-slate-200 ml-32" />
      </div>

      {/* RushTI 1.6 (Level-based) execution */}
      <div className="mb-8">
        <div className="flex items-center mb-3">
          <span className="text-sm font-semibold text-slate-700 bg-amber-50 px-3 py-1 rounded-full border border-amber-200">
            RushTI 1.6 (Levels)
          </span>
        </div>
        <div className="space-y-2">
          {levelExecution.map((task, idx) => {
            const colors = levelColors[tasks[idx].level]
            return (
              <div key={`level-${task.id}`} className="flex items-center gap-3">
                <div className="w-28 text-xs text-slate-500 text-right truncate">
                  {tasks[idx].name}
                </div>
                <div className="flex-1 h-7 bg-slate-50 rounded relative overflow-hidden">
                  {/* Background grid */}
                  <div className="absolute inset-0 flex">
                    {[...Array(14)].map((_, i) => (
                      <div key={i} className="flex-1 border-r border-slate-100 last:border-r-0" />
                    ))}
                  </div>
                  {/* Task bar */}
                  <div
                    className={`absolute h-full bg-gradient-to-r ${colors.from} ${colors.to} rounded transition-all duration-300 ease-out flex items-center justify-end pr-2`}
                    style={{
                      left: `${task.start * timeScale}%`,
                      width: `${getBarWidth(task.start, task.end, progress)}%`,
                      opacity: getBarOpacity(task.start, progress),
                    }}
                  >
                    {progress >= ((task.end / maxTime) * 100) && (
                      <span className={`text-[10px] ${colors.text} font-medium`}>{task.end - task.start}s</span>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
        {/* Total time indicator */}
        <div className="flex items-center gap-3 mt-3">
          <div className="w-28" />
          <div className="flex-1 flex justify-end">
            <div
              className={`text-sm font-semibold transition-all duration-500 ${isComplete ? 'text-amber-600' : 'text-slate-300'}`}
            >
              Total: {levelTotal}s
            </div>
          </div>
        </div>
      </div>

      {/* Divider */}
      <div className="relative my-6">
        <div className="h-px bg-slate-200" />
        <div className="absolute left-1/2 -translate-x-1/2 -top-3 bg-white px-4 text-xs text-slate-400 uppercase tracking-wider">
          vs
        </div>
      </div>

      {/* RushTI 2.0 (DAG-based) execution */}
      <div>
        <div className="flex items-center mb-3">
          <span className="text-sm font-semibold text-slate-700 bg-sky-50 px-3 py-1 rounded-full border border-sky-200">
            RushTI 2.0 (DAG)
          </span>
        </div>
        <div className="space-y-2">
          {dagExecution.map((task, idx) => {
            const colors = levelColors[tasks[idx].level]
            return (
              <div key={`dag-${task.id}`} className="flex items-center gap-3">
                <div className="w-28 text-xs text-slate-500 text-right truncate">
                  {tasks[idx].name}
                </div>
                <div className="flex-1 h-7 bg-slate-50 rounded relative overflow-hidden">
                  {/* Background grid */}
                  <div className="absolute inset-0 flex">
                    {[...Array(14)].map((_, i) => (
                      <div key={i} className="flex-1 border-r border-slate-100 last:border-r-0" />
                    ))}
                  </div>
                  {/* Task bar */}
                  <div
                    className={`absolute h-full bg-gradient-to-r ${colors.from} ${colors.to} rounded transition-all duration-300 ease-out flex items-center justify-end pr-2`}
                    style={{
                      left: `${task.start * timeScale}%`,
                      width: `${getBarWidth(task.start, task.end, progress)}%`,
                      opacity: getBarOpacity(task.start, progress),
                    }}
                  >
                    {progress >= ((task.end / maxTime) * 100) && (
                      <span className={`text-[10px] ${colors.text} font-medium`}>{task.end - task.start}s</span>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
        {/* Total time indicator */}
        <div className="flex items-center gap-3 mt-3">
          <div className="w-28" />
          <div className="flex-1 flex justify-end">
            <div
              className={`text-sm font-semibold transition-all duration-500 ${isComplete ? 'text-sky-600' : 'text-slate-300'}`}
            >
              Total: {dagTotal}s
            </div>
          </div>
        </div>
      </div>

      {/* Performance comparison cards */}
      {isComplete && (
        <div className="mt-8 animate-fade-in">
          <div className="grid grid-cols-2 gap-4">
            {/* Before card */}
            <div className="relative p-5 bg-slate-100 rounded-xl border border-slate-200">
              <div className="absolute top-3 left-3">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Before</span>
              </div>
              <div className="pt-4 text-center">
                <div className="text-4xl font-bold text-slate-400 mb-1">{levelTotal}s</div>
                <div className="text-xs text-slate-400">Level-based execution</div>
              </div>
            </div>

            {/* After card */}
            <div className="relative p-5 bg-gradient-to-br from-emerald-50 to-sky-50 rounded-xl border border-emerald-200 overflow-hidden">
              {/* Glow effect */}
              <div className="absolute -top-4 -right-4 w-20 h-20 bg-emerald-400/20 rounded-full blur-xl" />
              <div className="absolute top-3 left-3">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-emerald-600">After</span>
              </div>
              <div className="pt-4 text-center relative">
                <div className="text-4xl font-bold text-emerald-600 mb-1">{dagTotal}s</div>
                <div className="text-xs text-emerald-600">DAG-based execution</div>
              </div>
            </div>
          </div>

          {/* Improvement badge */}
          <div className="flex justify-center -mt-3 relative z-10">
            <div className="inline-flex items-center gap-3 px-5 py-2.5 bg-white rounded-full shadow-lg border border-slate-200">
              <div className="flex items-center gap-1.5">
                <svg className="w-4 h-4 text-emerald-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
                <span className="text-sm font-bold text-emerald-600">50% faster</span>
              </div>
              <div className="w-px h-4 bg-slate-200" />
              <div className="flex items-center gap-1.5">
                <svg className="w-4 h-4 text-sky-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <span className="text-sm font-bold text-sky-600">{levelTotal - dagTotal}s saved</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default function HowItWorks() {
  const [ref, isVisible] = useScrollAnimation()

  return (
    <section id="how-it-works" className="py-24 relative bg-slate-50">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-12" ref={ref}>
          <div className={`inline-flex items-center space-x-2 bg-slate-100 border border-slate-200 rounded-full px-4 py-2 mb-6 animate-on-scroll ${isVisible ? 'visible' : ''}`}>
            <svg className="w-4 h-4 text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
            </svg>
            <span className="text-slate-700 text-sm font-medium">Under the Hood</span>
          </div>
          <h2 className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-4 animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '50ms' }}>
            Re-engineered from the ground up
          </h2>
          <p className={`text-slate-600 text-lg max-w-2xl mx-auto animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '100ms' }}>
            We rebuilt how RushTI handles dependencies to maximize parallelization and infrastructure utilization.
            Tasks now execute the moment their dependencies complete; no more waiting for entire levels to finish.
          </p>
        </div>

        {/* Timeline visualization */}
        <div className={`animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '200ms' }}>
          <ExecutionTimeline />
        </div>

        {/* Key benefits */}
        <div className={`grid grid-cols-1 md:grid-cols-3 gap-6 mt-12 animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '300ms' }}>
          <div className="text-center p-6 bg-white rounded-xl border border-slate-200 shadow-sm">
            <div className="text-4xl font-bold bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent mb-2">Automatic</div>
            <p className="text-slate-600">Dependency detection from your task definitions</p>
          </div>
          <div className="text-center p-6 bg-white rounded-xl border border-slate-200 shadow-sm">
            <div className="text-4xl font-bold bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent mb-2">Optimized</div>
            <p className="text-slate-600">Tasks scheduled based on learned execution times</p>
          </div>
          <div className="text-center p-6 bg-white rounded-xl border border-slate-200 shadow-sm">
            <div className="text-4xl font-bold bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent mb-2">Resilient</div>
            <p className="text-slate-600">Failed tasks don't block independent workflows</p>
          </div>
        </div>
      </div>
    </section>
  )
}
