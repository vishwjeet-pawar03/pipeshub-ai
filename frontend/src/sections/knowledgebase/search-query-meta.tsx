import React from 'react';
import { Icon } from '@iconify/react';

import { 
  Box, 
  Chip, 
  Paper, 
  alpha, 
  Stack, 
  useTheme, 
  Typography
} from '@mui/material';

interface SearchMetadataProps {
  searchQuery: string;
  searchDate: string;
  citationCount: number;
  isArchived?: boolean;
  isShared?: boolean;
  formatDate: (date: string) => string;
}

export const SearchQueryMetadata = ({
  searchQuery,
  searchDate,
  citationCount,
  isArchived = false,
  isShared = false,
  formatDate
}: SearchMetadataProps) => {
  const theme = useTheme();
  
  return (
    <Paper
      elevation={0}
      sx={{
        overflow: 'hidden',
        borderRadius: '16px',
        border: `1px solid ${alpha(theme.palette.divider, 0.06)}`,
        background: theme.palette.mode === 'dark'
          ? 'linear-gradient(145deg, rgba(27,32,43,0.6) 0%, rgba(27,32,43,0.4) 100%)'
          : 'linear-gradient(145deg, rgba(255,255,255,0.7) 0%, rgba(255,255,255,0.95) 100%)',
        backdropFilter: 'blur(10px)',
        transition: 'all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1)',
        '&:hover': {
          boxShadow: `0 6px 24px ${alpha(theme.palette.common.black, theme.palette.mode === 'dark' ? 0.35 : 0.05)}`,
          transform: 'translateY(-2px)'
        }
      }}
    >
      {/* Query Header */}
      <Box
        sx={{
          px: 3.5,
          py: 2.25,
          borderBottom: `1px solid ${alpha(theme.palette.divider, 0.08)}`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          backdropFilter: 'blur(8px)',
          position: 'relative',
          '&::after': {
            content: '""',
            position: 'absolute',
            bottom: 0,
            left: '50%',
            width: '40%',
            height: '1px',
            bgcolor: alpha(theme.palette.primary.main, 0.3),
            transform: 'translateX(-50%)'
          }
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 28,
              height: 28,
              borderRadius: '8px',
              background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.2)} 0%, ${alpha(theme.palette.primary.main, 0.05)} 100%)`,
            }}
          >
            <Icon 
              icon="eva:search-fill" 
              width={16} 
              style={{
                color: theme.palette.primary.main,
              }}
            />
          </Box>
          <Typography
            variant="subtitle2"
            sx={{
              fontSize: '0.875rem',
              fontWeight: 700,
              letterSpacing: '0.02em',
              color: theme.palette.text.primary,
              fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
            }}
          >
            SEARCH QUERY
          </Typography>
        </Box>

        {/* Citations Count Badge */}
        <Chip
          icon={
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: 20,
                height: 20,
                borderRadius: '6px',
                background: alpha(theme.palette.info.main, 0.2),
                ml: 0.5
              }}
            >
              <Icon 
                icon="eva:file-text-fill" 
                style={{ 
                  fontSize: '12px',
                  color: theme.palette.info.main
                }} 
              />
            </Box>
          }
          label={`${citationCount} Citations`}
          color="info"
          size="small"
          variant="filled"
          sx={{
            height: 30,
            fontSize: '0.8125rem',
            fontWeight: 600,
            borderRadius: '10px',
            bgcolor: theme.palette.info.main,
            color: '#fff',
            border: 'none',
            px: 1,
            fontFamily: "'Inter', system-ui, sans-serif",
            boxShadow: `0 2px 8px ${alpha(theme.palette.info.main, 0.25)}`,
            '& .MuiChip-icon': {
              marginLeft: '2px',
              marginRight: '-2px',
            },
            transition: 'all 0.2s ease',
            '&:hover': {
              boxShadow: `0 4px 12px ${alpha(theme.palette.info.main, 0.35)}`,
              bgcolor: theme.palette.info.dark,
            }
          }}
        />
      </Box>

      {/* Query Content */}
      <Box sx={{ px: 3.5, pt: 2.5, pb: 3 }}>
        <Box
          sx={{
            p: 2.5,
            borderRadius: '12px',
            bgcolor: theme.palette.mode === 'dark'
              ? alpha(theme.palette.background.default, 0.4) 
              : alpha(theme.palette.background.default, 0.6),
            border: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
            fontFamily: "'SF Mono', 'Roboto Mono', 'Fira Code', monospace",
            fontSize: '0.9375rem',
            color: theme.palette.mode === 'dark'
              ? alpha(theme.palette.common.white, 0.92)
              : alpha(theme.palette.common.black, 0.85),
            fontWeight: 500,
            lineHeight: 1.7,
            mb: 3.5,
            position: 'relative',
            overflow: 'hidden',
            whiteSpace: 'pre-wrap',
            boxShadow: `inset 0 1px 2px ${alpha(theme.palette.common.black, 0.06)}`,
            letterSpacing: '0.01em',
          }}
        >
          {searchQuery}
        </Box>

        {/* Status and Date */}
        <Box 
          sx={{ 
            display: 'flex', 
            alignItems: 'flex-start', 
            justifyContent: 'space-between',
            flexWrap: { xs: 'wrap', sm: 'nowrap' },
            gap: { xs: 3, sm: 6 },
            mt: 1
          }}
        >
          {/* Status Section */}
          <Stack direction="row" spacing={2} alignItems="center" sx={{ flex: '1 1 auto' }}>
            <Box
              sx={{
                width: 40,
                height: 40,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                borderRadius: '12px',
                background: `linear-gradient(135deg, ${alpha(theme.palette.success.main, 0.15)} 0%, ${alpha(theme.palette.success.main, 0.05)} 100%)`,
                boxShadow: `0 2px 6px ${alpha(theme.palette.success.main, 0.15)}`
              }}
            >
              <Icon
                icon="eva:shield-fill" 
                width={18}
                style={{ 
                  color: theme.palette.success.main,
                }}
              />
            </Box>
            <Box>
              <Typography
                variant="caption"
                sx={{
                  fontSize: '0.75rem',
                  fontWeight: 700,
                  color: alpha(theme.palette.text.secondary, 0.85),
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em',
                  display: 'block',
                  mb: 0.75,
                  fontFamily: "'Inter', system-ui, sans-serif"
                }}
              >
                STATUS
              </Typography>
              <Box sx={{ display: 'flex', gap: 1 }}>
                {isArchived && (
                  <Chip
                    label="Archived"
                    size="small"
                    sx={{
                      height: 26,
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      borderRadius: '8px',
                      bgcolor: theme.palette.mode === 'dark'
                        ? alpha(theme.palette.warning.main, 0.16)
                        : alpha(theme.palette.warning.main, 0.12),
                      color: theme.palette.warning.dark,
                      border: 'none',
                      fontFamily: "'Inter', system-ui, sans-serif",
                      px: 1.25
                    }}
                  />
                )}
                {isShared && (
                  <Chip
                    label="Shared"
                    size="small"
                    sx={{
                      height: 26,
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      borderRadius: '8px',
                      bgcolor: theme.palette.mode === 'dark'
                        ? alpha(theme.palette.info.main, 0.16)
                        : alpha(theme.palette.info.main, 0.12),
                      color: theme.palette.info.dark,
                      border: 'none',
                      fontFamily: "'Inter', system-ui, sans-serif",
                      px: 1.25
                    }}
                  />
                )}
                {!isArchived && !isShared && (
                  <Chip
                    label="Private"
                    size="small"
                    sx={{
                      height: 26,
                      fontSize: '0.75rem',
                      fontWeight: 600,
                      borderRadius: '8px',
                      bgcolor: theme.palette.mode === 'dark'
                        ? alpha(theme.palette.grey[500], 0.12)
                        : alpha(theme.palette.grey[500], 0.08),
                      color: theme.palette.text.secondary,
                      border: 'none',
                      fontFamily: "'Inter', system-ui, sans-serif",
                      px: 1.25
                    }}
                  />
                )}
              </Box>
            </Box>
          </Stack>

          {/* Date Section */}
          <Stack direction="row" spacing={2} alignItems="center" sx={{ flex: '1 1 auto' }}>
            <Box
              sx={{
                width: 40,
                height: 40,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                borderRadius: '12px',
                background: `linear-gradient(135deg, ${alpha(theme.palette.primary.main, 0.12)} 0%, ${alpha(theme.palette.primary.main, 0.03)} 100%)`,
                boxShadow: `0 2px 6px ${alpha(theme.palette.primary.main, 0.12)}`
              }}
            >
              <Icon
                icon="eva:calendar-outline" 
                width={18}
                style={{ 
                  color: theme.palette.mode === 'dark'
                    ? alpha(theme.palette.common.white, 0.85)
                    : theme.palette.primary.main,
                }}
              />
            </Box>
            <Box>
              <Typography
                variant="caption"
                sx={{
                  fontSize: '0.75rem',
                  fontWeight: 700,
                  color: alpha(theme.palette.text.secondary, 0.85),
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em',
                  display: 'block',
                  mb: 0.75,
                  fontFamily: "'Inter', system-ui, sans-serif"
                }}
              >
                SEARCH DATE
              </Typography>
              <Typography
                variant="body2"
                sx={{
                  fontSize: '0.875rem',
                  fontWeight: 600,
                  color: theme.palette.text.primary,
                  letterSpacing: '0.01em',
                  fontFamily: "'Inter', system-ui, sans-serif"
                }}
              >
                {searchDate ? formatDate(searchDate) : 'N/A'}
              </Typography>
            </Box>
          </Stack>
        </Box>
      </Box>
    </Paper>
  );
};