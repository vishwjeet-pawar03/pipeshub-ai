import React from 'react';
import { Icon } from '@iconify/react';

import { Box, Chip, alpha, Divider, Tooltip, useTheme, Typography } from '@mui/material';

import { ORIGIN } from './constants/knowledge-search';

// Types
import type { SearchResult } from './types/search-response';

interface SearchResultItemProps {
  result: SearchResult;
}

// Helper function to get file icon based on extension
const getFileIcon = (extension: string): string => {
  const ext = extension?.toLowerCase() || '';

  switch (ext) {
    case 'pdf':
      return 'mdi:file-pdf-box';
    case 'doc':
    case 'docx':
      return 'mdi:file-word-box';
    case 'xls':
    case 'xlsx':
    case 'csv':
      return 'mdi:file-excel-box';
    case 'ppt':
    case 'pptx':
      return 'mdi:file-powerpoint-box';
    case 'jpg':
    case 'jpeg':
    case 'png':
    case 'gif':
      return 'mdi:file-image-box';
    case 'zip':
    case 'rar':
    case '7z':
      return 'mdi:file-archive-box';
    case 'txt':
      return 'mdi:file-text-box';
    case 'html':
    case 'css':
    case 'js':
      return 'mdi:file-code-box';
    case 'mail':
    case 'email':
      return 'mdi:email';
    default:
      return 'mdi:file-document-box';
  }
};

const SearchResultItem = ({ result }: SearchResultItemProps) => {
  const theme = useTheme();
  
  if (!result?.metadata) return null;
  
  const sourceInfo = getSourceIcon(result, theme);
  const fileType = result.metadata.extension?.toUpperCase() || 'DOC';
  
  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', gap: 2 }}>
        {/* Document Icon */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 40,
            height: 40,
            borderRadius: '6px',
            bgcolor: alpha(getFileIconColor(result.metadata.extension || ''), 0.1),
            flexShrink: 0,
          }}
        >
          <Tooltip
            title={
              result.metadata.origin === ORIGIN.UPLOAD
                ? 'Local KB'
                : result.metadata.connector || result.metadata.origin || 'Document'
            }
          >
            <Icon icon={sourceInfo.icon} style={{ fontSize: 26, color: sourceInfo.color }} />
          </Tooltip>
        </Box>

        {/* Content */}
        <Box sx={{ flex: 1, minWidth: 0 }}>
          {/* Header with Title and Meta */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Typography variant="subtitle1" fontWeight={500} sx={{ pr: 2 }}>
              {result.metadata.recordName || 'Untitled Document'}
            </Typography>

            {/* Meta Icons */}
            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexShrink: 0 }}>
              <Chip
                label={fileType}
                size="small"
                sx={{
                  height: 20,
                  fontSize: '0.7rem',
                  borderRadius: '4px',
                }}
              />
            </Box>
          </Box>

          {/* Metadata Line */}
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', mt: 0.5, mb: 1 }}>
            {/* <Typography variant="caption" color="text.secondary">
              {formatDate(result.metadata. || new Date().toISOString())}
            </Typography> */}

            {/* <Divider orientation="vertical" flexItem sx={{ height: 12 }} /> */}

            <Typography variant="caption" color="text.secondary">
              {result.metadata.categories || 'General'}
            </Typography>

            {result.metadata.pageNum && (
              <>
                <Divider orientation="vertical" flexItem sx={{ height: 12 }} />
                <Typography variant="caption" color="text.secondary">
                  Page {result.metadata.pageNum[0]}
                </Typography>
              </>
            )}
          </Box>

          {/* Content Preview */}
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
            {getContentPreview(result.content)}
          </Typography>

          {/* Tags and Departments */}
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
            {result.metadata.topics &&
              result.metadata.topics.slice(0, 3).map((topic) => (
                <Chip
                  key={topic}
                  label={topic}
                  size="small"
                  sx={{
                    height: 20,
                    fontSize: '0.7rem',
                    borderRadius: '4px',
                  }}
                />
              ))}

            {result.metadata.departments &&
              result.metadata.departments.slice(0, 2).map((dept) => (
                <Chip
                  key={dept}
                  label={dept}
                  size="small"
                  variant="outlined"
                  sx={{
                    height: 20,
                    fontSize: '0.7rem',
                    borderRadius: '4px',
                  }}
                />
              ))}

            {((result.metadata.topics?.length || 0) > 3 || (result.metadata.departments?.length || 0) > 2) && (
              <Chip
                label={`+${
                  Math.max(0, (result.metadata.topics?.length || 0) - 3) +
                  Math.max(0, (result.metadata.departments?.length || 0) - 2)
                } more`}
                size="small"
                sx={{
                  height: 20,
                  fontSize: '0.7rem',
                  borderRadius: '4px',
                }}
              />
            )}
          </Box>
        </Box>
      </Box>
    </Box>
  );
};

export default SearchResultItem;

// Helper function to get file icon color based on extension
const getFileIconColor = (extension: string): string => {
  const ext = extension?.toLowerCase() || '';

  switch (ext) {
    case 'pdf':
      return '#f44336';
    case 'doc':
    case 'docx':
      return '#2196f3';
    case 'xls':
    case 'xlsx':
    case 'csv':
      return '#4caf50';
    case 'ppt':
    case 'pptx':
      return '#ff9800';
    case 'mail':
    case 'email':
      return '#9C27B0';
    default:
      return '#1976d2';
  }
};

// Helper function to format date strings
const formatDate = (dateString: string): string => {
  if (!dateString) return 'N/A';

  try {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch (e) {
    return 'N/A';
  }
};

// Generate a truncated preview of the content
const getContentPreview = (content: string, maxLength: number = 220): string => {
  if (!content) return '';
  return content.length > maxLength ? `${content.substring(0, maxLength)}...` : content;
};

// Get source icon based on origin/connector
const getSourceIcon = (
  result: SearchResult,
  theme: any
): { icon: string; color: string } => {
  if (!result?.metadata) {
    return { icon: 'mdi:database', color: theme.palette.text.secondary };
  }

  if (result.metadata.recordType === 'MAIL' || result.metadata.connector === 'GMAIL') {
    return { icon: 'mdi:gmail', color: '#EA4335' };
  }

  switch (result.metadata.connector) {
    case 'DRIVE':
      return { icon: 'mdi:google-drive', color: '#4285F4' };
    case 'SLACK':
      return { icon: 'mdi:slack', color: '#4A154B' };
    case 'JIRA':
      return { icon: 'mdi:jira', color: '#0052CC' };
    case 'TEAMS':
      return { icon: 'mdi:microsoft-teams', color: '#6264A7' };
    case 'ONEDRIVE':
      return { icon: 'mdi:microsoft-onedrive', color: '#0078D4' };
    default:
      if (result.metadata.origin === ORIGIN.UPLOAD) {
        return { icon: 'mdi:database', color: theme.palette.primary.main };
      }
      return { icon: 'mdi:database', color: theme.palette.text.secondary };
  }
};