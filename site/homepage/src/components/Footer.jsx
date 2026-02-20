import { Github, Package, ExternalLink } from 'lucide-react'
import { RushTILogo } from './logos/RushTILogo'
import { CubewiseLogo } from './logos/CubewiseLogo'

export default function Footer() {
  return (
    <footer className="py-12 bg-slate-50 border-t border-slate-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          {/* RushTI Brand */}
          <div className="md:col-span-2">
            <div className="mb-4">
              <RushTILogo height={36} />
            </div>
            <p className="text-slate-600 max-w-sm mb-4">
              Parallel TI execution engine for IBM Planning Analytics.
              Open source and free to use.
            </p>
            <div className="flex items-center space-x-4">
              <a
                href="https://github.com/cubewise-code/rushti"
                target="_blank"
                rel="noopener noreferrer"
                className="text-slate-400 hover:text-slate-700 transition-colors"
              >
                <Github className="w-5 h-5" />
              </a>
              <a
                href="https://pypi.org/project/rushti/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-slate-400 hover:text-slate-700 transition-colors"
              >
                <Package className="w-5 h-5" />
              </a>
            </div>
          </div>

          {/* Documentation */}
          <div>
            <h4 className="text-slate-900 font-semibold mb-4">Documentation</h4>
            <ul className="space-y-2">
              <li>
                <a href="/rushti/docs/getting-started/installation/" className="text-slate-600 hover:text-sky-600 transition-colors">
                  Installation
                </a>
              </li>
              <li>
                <a href="/rushti/docs/getting-started/quick-start/" className="text-slate-600 hover:text-sky-600 transition-colors">
                  Quick Start
                </a>
              </li>
              <li>
                <a href="/rushti/docs/getting-started/task-files/" className="text-slate-600 hover:text-sky-600 transition-colors">
                  Task Files
                </a>
              </li>
              <li>
                <a href="/rushti/docs/advanced/cli-reference/" className="text-slate-600 hover:text-sky-600 transition-colors">
                  CLI Reference
                </a>
              </li>
            </ul>
          </div>

          {/* Resources */}
          <div>
            <h4 className="text-slate-900 font-semibold mb-4">Resources</h4>
            <ul className="space-y-2">
              <li>
                <a
                  href="https://github.com/cubewise-code/rushti"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-slate-600 hover:text-sky-600 transition-colors inline-flex items-center space-x-1"
                >
                  <span>GitHub</span>
                  <ExternalLink className="w-3 h-3" />
                </a>
              </li>
              <li>
                <a
                  href="https://pypi.org/project/rushti/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-slate-600 hover:text-sky-600 transition-colors inline-flex items-center space-x-1"
                >
                  <span>PyPI</span>
                  <ExternalLink className="w-3 h-3" />
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/cubewise-code/rushti/issues"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-slate-600 hover:text-sky-600 transition-colors inline-flex items-center space-x-1"
                >
                  <span>Report Issues</span>
                  <ExternalLink className="w-3 h-3" />
                </a>
              </li>
              <li>
                <a
                  href="https://www.cubewise.com"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-slate-600 hover:text-sky-600 transition-colors inline-flex items-center space-x-1"
                >
                  <span>Cubewise</span>
                  <ExternalLink className="w-3 h-3" />
                </a>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom bar with both logos */}
        <div className="mt-12 pt-8 border-t border-slate-200 flex flex-col sm:flex-row justify-between items-center space-y-4 sm:space-y-0">
          <p className="text-slate-500 text-sm">
            &copy; {new Date().getFullYear()} Cubewise. Open source under MIT License.
          </p>
          <a
            href="https://www.cubewise.com"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center space-x-2 text-slate-500 hover:text-slate-700 transition-colors"
          >
            <span className="text-sm">A Cubewise Project</span>
            <CubewiseLogo height={24} />
          </a>
        </div>
      </div>
    </footer>
  )
}
