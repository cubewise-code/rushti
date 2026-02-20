import { Menu, X, Github, BookOpen } from 'lucide-react'
import { useState } from 'react'
import { RushTILogo } from './logos/RushTILogo'

export default function Navigation() {
  const [isMenuOpen, setIsMenuOpen] = useState(false)

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 bg-white/90 backdrop-blur-lg border-b border-slate-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo - RushTI only */}
          <div className="flex items-center">
            <a href="/rushti/" className="flex items-center">
              <RushTILogo height={28} />
            </a>
          </div>

          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center space-x-8">
            <a href="#features" className="text-slate-600 hover:text-sky-600 transition-colors">
              Features
            </a>
            <a href="#how-it-works" className="text-slate-600 hover:text-sky-600 transition-colors">
              How It Works
            </a>
            <a href="#use-cases" className="text-slate-600 hover:text-sky-600 transition-colors">
              Use Cases
            </a>
            <a
              href="/rushti/docs/"
              className="text-slate-600 hover:text-sky-600 transition-colors flex items-center space-x-1"
            >
              <BookOpen className="w-4 h-4" />
              <span>Docs</span>
            </a>
            <a
              href="https://github.com/cubewise-code/rushti"
              target="_blank"
              rel="noopener noreferrer"
              className="text-slate-600 hover:text-sky-600 transition-colors flex items-center space-x-1"
            >
              <Github className="w-4 h-4" />
              <span>GitHub</span>
            </a>
          </div>

          {/* Mobile menu button */}
          <div className="md:hidden">
            <button
              onClick={() => setIsMenuOpen(!isMenuOpen)}
              className="text-slate-600 hover:text-slate-900 p-2"
            >
              {isMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile menu */}
      {isMenuOpen && (
        <div className="md:hidden bg-white border-b border-slate-200">
          <div className="px-4 py-4 space-y-4">
            <a
              href="#features"
              className="block text-slate-600 hover:text-sky-600 transition-colors"
              onClick={() => setIsMenuOpen(false)}
            >
              Features
            </a>
            <a
              href="#how-it-works"
              className="block text-slate-600 hover:text-sky-600 transition-colors"
              onClick={() => setIsMenuOpen(false)}
            >
              How It Works
            </a>
            <a
              href="#use-cases"
              className="block text-slate-600 hover:text-sky-600 transition-colors"
              onClick={() => setIsMenuOpen(false)}
            >
              Use Cases
            </a>
            <a
              href="/rushti/docs/"
              className="block text-slate-600 hover:text-sky-600 transition-colors"
            >
              Documentation
            </a>
            <a
              href="https://github.com/cubewise-code/rushti"
              target="_blank"
              rel="noopener noreferrer"
              className="block text-slate-600 hover:text-sky-600 transition-colors"
            >
              GitHub
            </a>
          </div>
        </div>
      )}
    </nav>
  )
}
