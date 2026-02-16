import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { GitBranch, Zap, ArrowRight, Clock, Server, CheckCircle2 } from 'lucide-react'

// Animated flow diagram showing sequential → parallel transformation
function TransformationDiagram({ isVisible }) {
  const sequentialTasks = [
    { name: 'Load FX Rates', duration: '2s', color: 'sky' },
    { name: 'Load GL Data', duration: '3s', color: 'sky' },
    { name: 'Load Actuals', duration: '7s', color: 'sky' },
    { name: 'Calc Variance', duration: '5s', color: 'emerald' },
    { name: 'Build Reports', duration: '2s', color: 'violet' },
  ]

  const parallelLanes = [
    // Lane 1
    [
      { name: 'Load FX Rates', duration: '2s', color: 'sky', width: '28%' },
      { name: 'Calc Variance', duration: '5s', color: 'emerald', width: '72%' },
    ],
    // Lane 2
    [
      { name: 'Load GL Data', duration: '3s', color: 'sky', width: '43%' },
      { name: 'Build Reports', duration: '2s', color: 'violet', width: '28%' },
    ],
    // Lane 3
    [
      { name: 'Load Actuals', duration: '7s', color: 'sky', width: '100%' },
    ],
  ]

  const colorMap = {
    sky: {
      bg: 'bg-sky-500',
      light: 'bg-sky-50',
      border: 'border-sky-200',
      text: 'text-sky-700',
      gradient: 'from-sky-500 to-sky-400',
    },
    emerald: {
      bg: 'bg-emerald-500',
      light: 'bg-emerald-50',
      border: 'border-emerald-200',
      text: 'text-emerald-700',
      gradient: 'from-emerald-500 to-emerald-400',
    },
    violet: {
      bg: 'bg-violet-500',
      light: 'bg-violet-50',
      border: 'border-violet-200',
      text: 'text-violet-700',
      gradient: 'from-violet-500 to-violet-400',
    },
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-11 gap-6 lg:gap-4 items-center">
      {/* Before - Sequential */}
      <div
        className={`lg:col-span-5 transition-all duration-700 ${isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-8'}`}
        style={{ transitionDelay: '300ms' }}
      >
        <div className="bg-white rounded-2xl border border-slate-200 p-6 shadow-sm">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-amber-400" />
              <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Sequential</span>
            </div>
            <div className="flex items-center gap-1.5 px-2.5 py-1 bg-red-50 rounded-full border border-red-100">
              <Clock className="w-3 h-3 text-red-500" />
              <span className="text-xs font-bold text-red-600">19s total</span>
            </div>
          </div>

          {/* Sequential task bars stacked vertically */}
          <div className="space-y-2">
            {sequentialTasks.map((task, i) => {
              const colors = colorMap[task.color]
              return (
                <div
                  key={task.name}
                  className="flex items-center gap-3"
                  style={{
                    opacity: isVisible ? 1 : 0,
                    transform: isVisible ? 'translateX(0)' : 'translateX(-20px)',
                    transition: 'all 0.5s ease-out',
                    transitionDelay: `${400 + i * 100}ms`,
                  }}
                >
                  <div className="w-2 text-[10px] text-slate-400 text-right">{i + 1}</div>
                  <div className="flex-1 relative">
                    <div className={`h-9 bg-gradient-to-r ${colors.gradient} rounded-lg flex items-center px-3 justify-between`}>
                      <span className="text-[11px] font-medium text-white truncate">{task.name}</span>
                      <span className="text-[10px] text-white/80 font-mono flex-shrink-0 ml-2">{task.duration}</span>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>

          <div className="mt-4 pt-3 border-t border-slate-100 text-center">
            <span className="text-xs text-slate-400">Each task waits for the previous one to complete</span>
          </div>
        </div>
      </div>

      {/* Arrow in the middle */}
      <div
        className={`lg:col-span-1 flex items-center justify-center transition-all duration-700 ${isVisible ? 'opacity-100 scale-100' : 'opacity-0 scale-50'}`}
        style={{ transitionDelay: '600ms' }}
      >
        <div className="flex flex-col items-center gap-2">
          <div className="w-12 h-12 rounded-full bg-gradient-to-br from-sky-500 to-cyan-500 flex items-center justify-center shadow-lg shadow-sky-500/20">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <ArrowRight className="w-5 h-5 text-slate-300 rotate-90 lg:rotate-0" />
        </div>
      </div>

      {/* After - Parallel DAG */}
      <div
        className={`lg:col-span-5 transition-all duration-700 ${isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-8'}`}
        style={{ transitionDelay: '500ms' }}
      >
        <div className="bg-white rounded-2xl border border-slate-200 p-6 shadow-sm relative overflow-hidden">
          {/* Subtle glow */}
          <div className="absolute -top-10 -right-10 w-40 h-40 bg-emerald-400/10 rounded-full blur-3xl" />

          <div className="relative">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-emerald-400" />
                <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Parallel DAG</span>
              </div>
              <div className="flex items-center gap-1.5 px-2.5 py-1 bg-emerald-50 rounded-full border border-emerald-100">
                <Zap className="w-3 h-3 text-emerald-500" />
                <span className="text-xs font-bold text-emerald-600">7s total</span>
              </div>
            </div>

            {/* Parallel lanes */}
            <div className="space-y-2">
              {parallelLanes.map((lane, laneIdx) => (
                <div key={laneIdx} className="flex items-center gap-3">
                  <div className="w-2 text-[10px] text-slate-400 text-right">W{laneIdx + 1}</div>
                  <div className="flex-1 flex gap-1">
                    {lane.map((task, taskIdx) => {
                      const colors = colorMap[task.color]
                      return (
                        <div
                          key={task.name}
                          className="relative"
                          style={{
                            width: task.width,
                            opacity: isVisible ? 1 : 0,
                            transform: isVisible ? 'scaleX(1)' : 'scaleX(0)',
                            transformOrigin: 'left',
                            transition: 'all 0.6s ease-out',
                            transitionDelay: `${700 + laneIdx * 100 + taskIdx * 150}ms`,
                          }}
                        >
                          <div className={`h-9 bg-gradient-to-r ${colors.gradient} rounded-lg flex items-center px-3 justify-between`}>
                            <span className="text-[11px] font-medium text-white truncate">{task.name}</span>
                            <span className="text-[10px] text-white/80 font-mono flex-shrink-0 ml-1">{task.duration}</span>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-4 pt-3 border-t border-slate-100 text-center">
              <span className="text-xs text-slate-400">Independent tasks run simultaneously across workers</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default function WhatIsRushTI() {
  const [ref, isVisible] = useScrollAnimation()

  return (
    <section className="py-24 relative bg-white overflow-hidden">
      {/* Subtle background decoration */}
      <div className="absolute w-[600px] h-[600px] rounded-full bg-sky-400/3 blur-3xl -top-20 -right-40" />
      <div className="absolute w-[400px] h-[400px] rounded-full bg-emerald-400/3 blur-3xl bottom-0 -left-20" />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16" ref={ref}>
          <div
            className={`inline-flex items-center space-x-2 bg-sky-50 border border-sky-200 rounded-full px-4 py-2 mb-6 transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'}`}
          >
            <Zap className="w-4 h-4 text-sky-600" />
            <span className="text-sky-700 text-sm font-medium">What is RushTI?</span>
          </div>

          <h2
            className={`text-3xl sm:text-4xl lg:text-5xl font-bold text-slate-900 mb-6 leading-tight transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
            style={{ transitionDelay: '100ms' }}
          >
            Turn sequential TI processes
            <br />
            <span className="bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent">into parallel workflows</span>
          </h2>

          <p
            className={`text-slate-600 text-lg max-w-3xl mx-auto leading-relaxed transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
            style={{ transitionDelay: '200ms' }}
          >
            RushTI is an open-source execution engine for IBM Planning Analytics that replaces
            sequential TurboIntegrator execution with intelligent, dependency-aware parallel scheduling.
            Define your tasks, declare their dependencies, and let RushTI handle the rest.
          </p>
        </div>

        {/* Transformation diagram */}
        <div className="mb-12">
          <TransformationDiagram isVisible={isVisible} />
        </div>

        {/* Stat banner */}
        <div
          className={`grid grid-cols-2 md:grid-cols-4 gap-4 transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
          style={{ transitionDelay: '800ms' }}
        >
          {[
            { value: 'Multi', label: 'TM1 Instance Support', icon: Server },
            { value: 'DAG', label: 'Dependency Engine', icon: GitBranch },
            { value: '100%', label: 'Backwards Compatible', icon: CheckCircle2 },
            { value: 'Free', label: 'Open Source', icon: Zap },
          ].map((stat) => (
            <div key={stat.label} className="text-center p-5 bg-slate-50 rounded-xl border border-slate-100">
              <stat.icon className="w-5 h-5 text-sky-500 mx-auto mb-2" />
              <div className="text-2xl font-bold text-slate-900">{stat.value}</div>
              <div className="text-xs text-slate-500 mt-1">{stat.label}</div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
